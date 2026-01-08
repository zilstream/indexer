package prices

import (
	"context"
	"database/sql"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TokenRouter provides token->USD pricing at a point in time.
// It prefers direct stablecoin valuations, then ZIL routing, and can fall back to
// spot prices from V2 reserves or V3 sqrt_price_x96 (current table values).
// Returns decimal string for USD price per 1 token unit.
type TokenRouter interface {
	PriceTokenUSD(ctx context.Context, token string, ts time.Time) (price string, ok bool)
}

// DBRouter is a Postgres-backed implementation.
// Notes:
// - stablecoins are treated as $1.00 (normalized by token decimals)
// - wzil is routed via PriceZILUSD
// - one-hop attempts:
//   * V2: reserve-based spot to WZIL or stablecoin
//   * V3: sqrt_price_x96 spot from uniswap_v3_pools to WZIL or stablecoin
// The implementation aims to be simple and safe; it requires non-zero liquidity.
type DBRouter struct {
	pool         *pgxpool.Pool
	zilProvider  Provider
	wzil         string          // lowercase hex
	stable       map[string]bool // lowercase hex

	muDecimals sync.RWMutex
	decimals   map[string]int // token -> decimals cache
}

func NewDBRouter(pool *pgxpool.Pool, zil Provider, wzil string, stablecoins []string) *DBRouter {
	s := make(map[string]bool, len(stablecoins))
	for _, a := range stablecoins {
		s[strings.ToLower(a)] = true
	}
	return &DBRouter{
		pool:        pool,
		zilProvider: zil,
		wzil:        strings.ToLower(wzil),
		stable:      s,
		decimals:    make(map[string]int, 256),
	}
}

func (r *DBRouter) PriceTokenUSD(ctx context.Context, token string, ts time.Time) (string, bool) {
	addr := strings.ToLower(token)
	// Stablecoin → $1
	if r.stable[addr] {
		return "1", true
	}
	// WZIL → ZIL price
	if addr == r.wzil {
		return r.zilProvider.PriceZILUSD(ctx, ts)
	}
	// Try direct to WZIL via V2 reserves
	if p, ok := r.v2SpotToUSD(ctx, addr, ts); ok {
		if sanitizePrice(p) {
			return p, true
		}
	}
	// Try direct to WZIL/stable via V3 sqrt price
	if p, ok := r.v3SpotToUSD(ctx, addr, ts); ok {
		if sanitizePrice(p) {
			return p, true
		}
	}
	return "", false
}

// v2SpotToUSD tries to find a Uniswap V2 pair with WZIL or a stablecoin and derive USD.
func (r *DBRouter) v2SpotToUSD(ctx context.Context, token string, ts time.Time) (string, bool) {
	// Find any pair where token is token0 or token1 and the counter is wzil or stablecoin
	// Require minimum reserves and order by reserve_usd to prefer high-liquidity pools
	query := `
		SELECT token0, token1, reserve0::numeric, reserve1::numeric
		FROM uniswap_v2_pairs
		WHERE (token0 = $1 AND (token1 = $2 OR token1 = ANY($3)))
		   OR (token1 = $1 AND (token0 = $2 OR token0 = ANY($3)))
		ORDER BY COALESCE(reserve_usd, 0) DESC
		LIMIT 1`
	var t0, t1 string
	var r0, r1 sql.NullString
	stableList := r.stableList()
	if err := r.pool.QueryRow(ctx, query, token, r.wzil, stableList).Scan(&t0, &t1, &r0, &r1); err != nil {
		return "", false
	}
	if !r0.Valid || !r1.Valid {
		return "", false
	}
	// Require minimum reserves in the counter token (WZIL or stablecoin)
	// This prevents deriving prices from dust pools
	// Calculate minimum reserve based on counter token's decimals (0.1 tokens)
	d0 := r.getDecimals(ctx, t0)
	d1 := r.getDecimals(ctx, t1)
	if strings.ToLower(t0) == strings.ToLower(token) {
		// token is t0, counter is t1 - check r1 using t1's decimals
		minReserve := minReserveForDecimals(d1)
		if lessThan(r1.String, minReserve) {
			return "", false
		}
	} else {
		// token is t1, counter is t0 - check r0 using t0's decimals
		minReserve := minReserveForDecimals(d0)
		if lessThan(r0.String, minReserve) {
			return "", false
		}
	}
	// Adjust by decimals: price(token0 in token1) = (r1/10^d1) / (r0/10^d0) = (r1/r0) * 10^(d0-d1)
	if strings.ToLower(t0) == strings.ToLower(token) {
		pBase, ok := divStrings(r1.String, r0.String)
		if !ok { return "", false }
		// scale by 10^(d0-d1)
		scale := pow10BigRat(d0 - d1)
		p := new(big.Rat)
		if _, ok := p.SetString(pBase); !ok { return "", false }
		p.Mul(p, scale)
		return r.toUSD(ctx, strings.ToLower(t1), ts, p.FloatString(18))
	}
	// token is token1: price(token1 in token0) = (r0/r1) * 10^(d1-d0)
	pBase, ok := divStrings(r0.String, r1.String)
	if !ok { return "", false }
	scale := pow10BigRat(d1 - d0)
	p := new(big.Rat)
	if _, ok := p.SetString(pBase); !ok { return "", false }
	p.Mul(p, scale)
	return r.toUSD(ctx, strings.ToLower(t0), ts, p.FloatString(18))
}

// v3SpotToUSD tries via a V3 pool spot price in uniswap_v3_pools (sqrt_price_x96) to WZIL or stablecoin.
func (r *DBRouter) v3SpotToUSD(ctx context.Context, token string, ts time.Time) (string, bool) {
	// Order by liquidity to prefer high-TVL pools and require minimum reserves
	q := `
		SELECT token0, token1, sqrt_price_x96::numeric, reserve0::numeric, reserve1::numeric
		FROM uniswap_v3_pools
		WHERE (token0 = $1 AND (token1 = $2 OR token1 = ANY($3)))
		   OR (token1 = $1 AND (token0 = $2 OR token0 = ANY($3)))
		ORDER BY COALESCE(liquidity, 0) DESC
		LIMIT 1`
	var t0, t1 string
	var sp, r0, r1 sql.NullString
	stableList := r.stableList()
	if err := r.pool.QueryRow(ctx, q, token, r.wzil, stableList).Scan(&t0, &t1, &sp, &r0, &r1); err != nil {
		return "", false
	}
	if !sp.Valid || !r0.Valid || !r1.Valid { return "", false }
	// Require minimum reserves in counter token using its decimals
	dec0 := r.getDecimals(ctx, t0)
	dec1 := r.getDecimals(ctx, t1)
	if strings.ToLower(t0) == strings.ToLower(token) {
		// token is t0, counter is t1 - check r1 using t1's decimals
		minReserve := minReserveForDecimals(dec1)
		if lessThan(r1.String, minReserve) {
			return "", false
		}
	} else {
		// token is t1, counter is t0 - check r0 using t0's decimals
		minReserve := minReserveForDecimals(dec0)
		if lessThan(r0.String, minReserve) {
			return "", false
		}
	}
	sqrtStr := sp.String
	// Convert sqrt_price_x96 to price0 and price1 with decimals
	price0, price1, ok := pricesFromSqrtX96(sqrtStr, dec0, dec1)
	if !ok { return "", false }
	if strings.ToLower(t0) == strings.ToLower(token) {
		return r.toUSD(ctx, strings.ToLower(t1), ts, price0)
	}
	return r.toUSD(ctx, strings.ToLower(t0), ts, price1)
}

// toUSD converts a price in counter token units into USD if counter is WZIL or stablecoin
func (r *DBRouter) toUSD(ctx context.Context, counter string, ts time.Time, priceInCounter string) (string, bool) {
	// counter is stablecoin => USD directly
	if r.stable[counter] {
		return priceInCounter, true
	}
	// counter is WZIL => multiply by ZIL price
	if counter == r.wzil {
		zilUsd, ok := r.zilProvider.PriceZILUSD(ctx, ts)
		if !ok { return "", false }
		prod, ok := mulStrings(priceInCounter, zilUsd)
		return prod, ok
	}
	return "", false
}

func (r *DBRouter) stableList() []string {
	lst := make([]string, 0, len(r.stable))
	for a := range r.stable { lst = append(lst, a) }
	return lst
}

func (r *DBRouter) getDecimals(ctx context.Context, token string) int {
	addr := strings.ToLower(token)
	r.muDecimals.RLock()
	if d, ok := r.decimals[addr]; ok { r.muDecimals.RUnlock(); return d }
	r.muDecimals.RUnlock()
	var d sql.NullInt32
	_ = r.pool.QueryRow(ctx, `SELECT decimals FROM tokens WHERE address = $1`, addr).Scan(&d)
	val := 18
	if d.Valid { val = int(d.Int32) }
	r.muDecimals.Lock()
	r.decimals[addr] = val
	r.muDecimals.Unlock()
	return val
}

// pricesFromSqrtX96 returns price0 (token0 in token1) and price1 (token1 in token0) as decimal strings.
func pricesFromSqrtX96(sqrtStr string, dec0, dec1 int) (price0, price1 string, ok bool) {
	// price0 = (sqrt^2 / 2^192) * 10^(dec0-dec1)
	// Using big.Rat for precision
	sqrt := new(big.Int)
	_, ok = sqrt.SetString(sqrtStr, 10)
	if !ok { return "","", false }
	num := new(big.Int).Mul(sqrt, sqrt)              // sqrt^2
	den := new(big.Int).Lsh(big.NewInt(1), 192)     // 2^192
	ratio := new(big.Rat).SetFrac(num, den)
	// scale by decimals (normalize to human units)
	pow10 := pow10BigRat(dec0 - dec1)
	ratio.Mul(ratio, pow10)
	price0 = ratio.FloatString(18)
	// price1 is inverse adjusted: 1/price0
	if ratio.Sign() == 0 { return "","", false }
	inv := new(big.Rat).Inv(ratio)
	price1 = inv.FloatString(18)
	return price0, price1, true
}

// pow10BigRat returns 10^n as a big.Rat with exact precision (no float64 loss).
func pow10BigRat(n int) *big.Rat {
	if n == 0 {
		return big.NewRat(1, 1)
	}
	// 10^n = (10^abs(n), 1) if n>0, else (1, 10^abs(n))
	abs := n
	if abs < 0 {
		abs = -abs
	}
	ten := big.NewInt(10)
	pow := new(big.Int).Exp(ten, big.NewInt(int64(abs)), nil)
	if n > 0 {
		return new(big.Rat).SetFrac(pow, big.NewInt(1))
	}
	return new(big.Rat).SetFrac(big.NewInt(1), pow)
}

func divStrings(a, b string) (string, bool) {
	x := new(big.Rat)
	if _, ok := x.SetString(a); !ok { return "", false }
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok { return "", false }
	if y.Sign() == 0 { return "", false }
	x.Quo(x, y)
	return x.FloatString(18), true
}

func mulStrings(a, b string) (string, bool) {
	x := new(big.Rat)
	if _, ok := x.SetString(a); !ok { return "", false }
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok { return "", false }
	x.Mul(x, y)
	return x.FloatString(18), true
}

func lessThan(a, b string) bool {
	x := new(big.Rat)
	if _, ok := x.SetString(a); !ok { return false }
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok { return false }
	return x.Cmp(y) < 0
}

// minReserveForDecimals returns minimum reserve (0.1 tokens) as a string for the given decimals.
// E.g., for 18 decimals: 0.1 * 10^18 = 100000000000000000
// For 6 decimals: 0.1 * 10^6 = 100000
func minReserveForDecimals(decimals int) string {
	// 0.1 tokens = 10^(decimals-1)
	if decimals <= 0 {
		return "0"
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals-1)), nil)
	return pow.String()
}

// sanitizePrice rejects prices that are unreasonably high (likely due to low liquidity)
// Max price of $10M per token is generous but prevents astronomical values
func sanitizePrice(price string) bool {
	maxPrice := "10000000" // $10M per token
	p := new(big.Rat)
	if _, ok := p.SetString(price); !ok { return false }
	max := new(big.Rat)
	if _, ok := max.SetString(maxPrice); !ok { return false }
	return p.Cmp(max) <= 0 && p.Sign() >= 0
}
