// Longbridge Gateway (Go) — 按需加载版 v2
//
// 极薄行情桥接：Longbridge WS → NATS
// 不做 Greeks 计算、不做状态管理——那些交给 Rust。
//
// NATS subjects:
//   quote.option.qqq               → QQQ 正股行情
//   quote.option.vix               → VIX 恐慌指数
//   kline.option.qqq               → QQQ 1 分钟 K 线
//   quote.option.{symbol_key}      → 单腿期权行情（含 IV、OI）
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/longbridge/openapi-go/config"
	"github.com/longbridge/openapi-go/oauth"
	"github.com/longbridge/openapi-go/quote"
	"github.com/nats-io/nats.go"
)

// ── 环境变量 ────────────────────────────────────────────

var (
	natsURL       = getEnv("NATS_URL", "nats://127.0.0.1:4222")
	oauthClientID = os.Getenv("LONGBRIDGE_OAUTH_CLIENT_ID")
	ivRefreshSecs = getEnvInt("IV_REFRESH_INTERVAL", 10)
	strikeWindow  = getEnvFloat("STRIKE_WINDOW", 5.0)
	rebalanceDelta = getEnvFloat("REBALANCE_DELTA", 2.0)
	qqqSymbol     = "QQQ.US"
	vixSymbol     = getEnv("VIX_SYMBOL", "VIX.US")
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" { return v }
	return def
}
func getEnvInt(key string, def int) int {
	v := os.Getenv(key); if v == "" { return def }
	var n int; fmt.Sscanf(v, "%d", &n); return n
}
func getEnvFloat(key string, def float64) float64 {
	v := os.Getenv(key); if v == "" { return def }
	var f float64; fmt.Sscanf(v, "%f", &f); return f
}

// ── JSON 消息结构 ────────────────────────────────────────

type optionQuoteMsg struct {
	Symbol       string           `json:"symbol"`
	LastDone     string           `json:"last_done"`
	PrevClose    string           `json:"prev_close"`
	Open         string           `json:"open"`
	High         string           `json:"high"`
	Low          string           `json:"low"`
	Timestamp    int64            `json:"timestamp"`
	Volume       int64            `json:"volume"`
	Turnover     string           `json:"turnover"`
	OptionExtend *optionExtendMsg `json:"option_extend,omitempty"`
}

type optionExtendMsg struct {
	ImpliedVolatility    string `json:"implied_volatility"`
	OpenInterest         int64  `json:"open_interest"`
	ExpiryDate           string `json:"expiry_date"`
	StrikePrice          string `json:"strike_price"`
	ContractMultiplier   string `json:"contract_multiplier"`
	ContractType         string `json:"contract_type"`
	ContractSize         string `json:"contract_size"`
	Direction            string `json:"direction"`
	HistoricalVolatility string `json:"historical_volatility"`
	UnderlyingSymbol     string `json:"underlying_symbol"`
}

// K 线消息
type klineMsg struct {
	Symbol    string  `json:"symbol"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    int64   `json:"volume"`
	Timestamp int64   `json:"timestamp"` // K 线开始时间的 unix 秒
}

// ── K 线构建器（tick 级实时）────────────────────────────

type klineBuilder struct {
	mu         sync.Mutex
	current    *klineMsg
	currentMin int64
	prevCumVol int64
}

func newKlineBuilder() *klineBuilder {
	return &klineBuilder{}
}

// update 用 QQQ 报价更新当前 K 线。分钟切换时直接返回上一根完整 K 线。
func (k *klineBuilder) update(px float64, cumVol int64, ts int64) *klineMsg {
	k.mu.Lock()
	defer k.mu.Unlock()

	thisMin := ts / 60 * 60

	// 分钟成交量
	var minVol int64
	if cumVol > k.prevCumVol {
		minVol = cumVol - k.prevCumVol
	}
	k.prevCumVol = cumVol

	// 分钟切换 → 返回上一根
	var completed *klineMsg
	if k.current != nil && thisMin != k.currentMin {
		completed = k.current
		k.current = nil
	}

	if k.current == nil {
		k.currentMin = thisMin
		k.current = &klineMsg{
			Symbol:    qqqSymbol,
			Open:      px,
			High:      px,
			Low:       px,
			Close:     px,
			Volume:    minVol,
			Timestamp: thisMin,
		}
		return completed
	}

	// 更新 OHLCV
	if px > k.current.High { k.current.High = px }
	if px < k.current.Low  { k.current.Low  = px }
	k.current.Close = px
	k.current.Volume += minVol

	return completed
}

// ── 辅助 ────────────────────────────────────────────────

func symbolKey(symbol string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(symbol) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b.WriteRune(c)
		}
	}
	return b.String()
}

func decStr(d interface{ String() string }) string {
	if d == nil { return "0" }
	rv := reflect.ValueOf(d)
	if rv.Kind() == reflect.Ptr && rv.IsNil() { return "0" }
	return d.String()
}

// ── 期权链（按需加载）───────────────────────────────────

type strikeEntry struct {
	price      float64
	callSymbol string
	putSymbol  string
}

type optionChain struct {
	mu         sync.Mutex
	expiryDate string
	allStrikes []strikeEntry
	activeKeys map[string]bool
	windowLow  float64
	windowHigh float64
}

func load0DTEExpiry(ctx context.Context, qc *quote.QuoteContext) (*time.Time, error) {
	dates, err := qc.OptionChainExpiryDateList(ctx, qqqSymbol)
	if err != nil { return nil, fmt.Errorf("获取到期日失败: %w", err) }
	if len(dates) == 0 { return nil, fmt.Errorf("QQQ 无可用到期日") }
	today := time.Now().Truncate(24 * time.Hour)
	for i := range dates {
		d := &dates[i]
		if d.Year() == today.Year() && d.Month() == today.Month() && d.Day() == today.Day() {
			log.Printf("[chain] 0DTE 到期日: %s", d.Format("2006-01-02"))
			return d, nil
		}
	}
	log.Printf("[chain] 可用到期日: %v", dates)
	return nil, fmt.Errorf("今日无 0DTE 到期日，拒绝启动（防止误交易非当日合约）")
}

func loadAllStrikes(ctx context.Context, qc *quote.QuoteContext, expiry *time.Time) ([]strikeEntry, error) {
	info, err := qc.OptionChainInfoByDate(ctx, qqqSymbol, expiry)
	if err != nil { return nil, fmt.Errorf("获取期权链失败: %w", err) }
	var strikes []strikeEntry
	for _, s := range info {
		if s.Price == nil { continue }
		price, _ := s.Price.Float64()
		strikes = append(strikes, strikeEntry{price: price, callSymbol: s.CallSymbol, putSymbol: s.PutSymbol})
	}
	sort.Slice(strikes, func(i, j int) bool { return strikes[i].price < strikes[j].price })
	log.Printf("[chain] 全链 %d 个行权价 (%.0f ~ %.0f)", len(strikes), strikes[0].price, strikes[len(strikes)-1].price)
	return strikes, nil
}

func (c *optionChain) selectWindow(currentPrice float64) []string {
	c.mu.Lock(); defer c.mu.Unlock()
	low := currentPrice - strikeWindow; high := currentPrice + strikeWindow
	c.windowLow = low; c.windowHigh = high
	seen := make(map[string]bool)
	var symbols []string
	for _, s := range c.allStrikes {
		if s.price < low || s.price > high { continue }
		if s.callSymbol != "" && !seen[s.callSymbol] { symbols = append(symbols, s.callSymbol); seen[s.callSymbol] = true }
		if s.putSymbol != "" && !seen[s.putSymbol] { symbols = append(symbols, s.putSymbol); seen[s.putSymbol] = true }
	}
	c.activeKeys = make(map[string]bool, len(symbols))
	for _, sym := range symbols { c.activeKeys[symbolKey(sym)] = true }
	return symbols
}

func (c *optionChain) shouldRebalance(currentPrice float64) bool {
	c.mu.Lock(); defer c.mu.Unlock()
	if c.windowLow == 0 && c.windowHigh == 0 { return true }
	return currentPrice < c.windowLow+rebalanceDelta || currentPrice > c.windowHigh-rebalanceDelta
}

// ── Longbridge 认证 ─────────────────────────────────────

func buildConfig(ctx context.Context) (*config.Config, error) {
	if oauthClientID != "" {
		log.Printf("[auth] 使用 OAuth 2.0")
		o := oauth.New(oauthClientID).OnOpenURL(func(url string) { fmt.Printf("[auth] 请打开浏览器授权:\n%s\n", url) })
		if err := o.Build(ctx); err != nil { return nil, fmt.Errorf("OAuth 失败: %w", err) }
		return config.New(config.WithOAuthClient(o))
	}
	log.Printf("[auth] 使用 API Key（环境变量）")
	return config.New()
}

// ── 主逻辑 ─────────────────────────────────────────────

func main() {
	log.SetFlags(log.Ltime)
	log.SetOutput(os.Stdout)
	log.Println("[gateway] Longbridge Gateway (Go) v2 — K线+VIX+按需期权")

	// 1. NATS
	log.Printf("[gateway] 连接 NATS: %s", natsURL)
	nc, err := nats.Connect(natsURL)
	if err != nil { log.Fatalf("连接 NATS 失败: %v", err) }
	defer nc.Close()

	// 2. Longbridge
	ctx := context.Background()
	cfg, err := buildConfig(ctx)
	if err != nil { log.Fatalf("认证失败: %v", err) }
	qc, err := quote.NewFromCfg(cfg)
	if err != nil { log.Fatalf("QuoteContext 失败: %v", err) }
	defer qc.Close()

	// 3. 期权链
	expiry, err := load0DTEExpiry(ctx, qc)
	if err != nil { log.Fatalf("到期日: %v", err) }
	chain := &optionChain{expiryDate: expiry.Format("2006-01-02")}
	chain.allStrikes, err = loadAllStrikes(ctx, qc, expiry)
	if err != nil { log.Fatalf("行权价: %v", err) }

	// 4. K 线构建器 + 行情处理
	kline := newKlineBuilder()
	var (
		qqqPrice    float64
		qqqPriceMu  sync.Mutex
		qqqReady    = make(chan struct{})
		subscribed  = false
		subscribeMu sync.Mutex
		currentSubs []string
	)

	qc.OnQuote(func(event *quote.PushQuote) {
		if event == nil || event.LastDone == nil { return }
		symbol := event.Symbol
		px, _ := event.LastDone.Float64()
		if px <= 0 { return }

		// 发布行情到 NATS
		msg := optionQuoteMsg{
			Symbol:    symbol,
			LastDone:  event.LastDone.String(),
			PrevClose: "0",
			Open:      decStr(event.Open),
			High:      decStr(event.High),
			Low:       decStr(event.Low),
			Timestamp: event.Timestamp,
			Volume:    event.Volume,
			Turnover:  decStr(event.Turnover),
		}
		subject := "quote.option." + symbolKey(symbol)
		if symbol == vixSymbol { subject = "quote.option.vix" }
		data, _ := json.Marshal(msg)
		go nc.Publish(subject, data)

		// QQQ 价格处理 → 更新 K 线 + 触发期权链
		if symbol == qqqSymbol {
			// tick 级实时 K 线：分钟切换时直接返回上一根
			if candle := kline.update(px, event.Volume, event.Timestamp); candle != nil {
		data, _ := json.Marshal(candle)
		go nc.Publish("kline.option.qqq", data)
			}

			qqqPriceMu.Lock()
			oldPrice := qqqPrice; qqqPrice = px
			qqqPriceMu.Unlock()

			select {
			case <-qqqReady:
			if chain.shouldRebalance(px) {
				newSyms := chain.selectWindow(px)
				subscribeMu.Lock()
				// 全量重建窗口（含缩窗）：price 回到原始区间时，currentSubs 也会同步收缩
				if subscribed {
					// 先扩再替：Longbridge Go SDK 可能不支持退订，但至少不再轮询过期标的
					existing := make(map[string]bool)
					for _, s := range currentSubs { existing[s] = true }
					var added []string
					for _, s := range newSyms { if !existing[s] { added = append(added, s) } }
					if len(added) > 0 {
						log.Printf("[gateway] 扩窗 +%d 个期权", len(added))
						qc.Subscribe(ctx, added, []quote.SubType{quote.SubTypeQuote}, false)
					}
					if len(newSyms) < len(currentSubs) {
						log.Printf("[gateway] 缩窗 %d→%d 个期权（price 回归区间）", len(currentSubs), len(newSyms))
					}
					// 关键：currentSubs 完全替换为新窗口，IV 轮询只拉当前窗口内标的
					currentSubs = newSyms
				}
				subscribeMu.Unlock()
			}
			default:
		if oldPrice > 0 || px <= 0 { return }
		log.Printf("[gateway] QQQ 现价: %.2f, 筛选 ±%.0f 期权...", px, strikeWindow)
		optSyms := chain.selectWindow(px)
		log.Printf("[gateway] 筛选出 %d 个期权 (%.0f ~ %.0f)", len(optSyms), chain.windowLow, chain.windowHigh)
		subscribeMu.Lock()
		qc.Subscribe(ctx, optSyms, []quote.SubType{quote.SubTypeQuote}, false)
		subscribed = true; currentSubs = optSyms
		subscribeMu.Unlock()
		close(qqqReady)
			}
		}
	})

	// 5. 订阅 QQQ + VIX
	log.Printf("[gateway] 订阅 QQQ.US + %s，等待现价...", vixSymbol)
	if err := qc.Subscribe(ctx, []string{qqqSymbol, vixSymbol}, []quote.SubType{quote.SubTypeQuote}, true); err != nil {
		log.Fatalf("订阅 QQQ+VIX 失败: %v", err)
	}

	// 6. 等待就绪
	select {
	case <-qqqReady:
		log.Println("[gateway] ✅ 行情桥接已就绪（按需窗口）")
	case <-time.After(30 * time.Second):
		log.Println("[gateway] ⚠️ 30s 超时，全量兜底")
		var allSyms []string
		for _, s := range chain.allStrikes {
			if s.callSymbol != "" { allSyms = append(allSyms, s.callSymbol) }
			if s.putSymbol != "" { allSyms = append(allSyms, s.putSymbol) }
		}
		qc.Subscribe(ctx, allSyms, []quote.SubType{quote.SubTypeQuote}, false)
		close(qqqReady)
	}

	// 7. IV/OI 定时拉取
	go func() {
		ticker := time.NewTicker(time.Duration(ivRefreshSecs) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			subscribeMu.Lock()
			syms := make([]string, len(currentSubs))
			copy(syms, currentSubs)
			subscribeMu.Unlock()
			if len(syms) == 0 { continue }
			quotes, err := qc.OptionQuote(ctx, syms)
			if err != nil { continue }
			for _, oq := range quotes {
		if oq == nil { continue }
		key := symbolKey(oq.Symbol)
		msg := optionQuoteMsg{
			Symbol: oq.Symbol, LastDone: decStr(oq.LastDone),
			PrevClose: decStr(oq.PrevClose), Open: decStr(oq.Open),
			High: decStr(oq.High), Low: decStr(oq.Low),
			Timestamp: oq.Timestamp, Volume: oq.Volume, Turnover: decStr(oq.Turnover),
		}
		if oq.OptionExtend != nil {
			ext := oq.OptionExtend
			msg.OptionExtend = &optionExtendMsg{
				ImpliedVolatility: ext.ImpliedVolatility, OpenInterest: ext.OpenInterest,
				ExpiryDate: ext.ExpiryDate, StrikePrice: decStr(ext.StrikePrice),
				ContractMultiplier: ext.ContractMultiplier, ContractType: ext.ContractType,
				ContractSize: ext.ContractSize, Direction: ext.Direction,
				HistoricalVolatility: ext.HistoricalVolatility, UnderlyingSymbol: ext.UnderlyingSymbol,
			}
		}
		data, _ := json.Marshal(msg)
		go nc.Publish("quote.option."+key, data)
			}
		}
	}()

	select {}
}
