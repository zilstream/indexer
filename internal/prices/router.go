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
// - stablecoins are priced via WZIL pools (ZIL-anchored)
// - wzil is routed via PriceZILUSD
// - one-hop attempts:
//   - V2: reserve-based spot to WZIL or stablecoin
//   - V3: sqrt_price_x96 spot from uniswap_v3_pools to WZIL or stablecoin
//
// The implementation aims to be simple and safe; it requires non-zero liquidity.
type DBRouter struct {
	pool        *pgxpool.Pool
	zilProvider Provider
	wzil        string          // lowercase hex
	stable      map[string]bool // lowercase hex

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
	// Stablecoin → derive from WZIL pair (not hardcoded $1)
	if r.stable[addr] {
		if p, ok := r.stablecoinPriceFromWZIL(ctx, addr, ts); ok {
			return p, true
		}
		return "", false
	}
	// WZIL → ZIL price
	if addr == r.wzil {
		return r.zilProvider.PriceZILUSD(ctx, ts)
	}
	return r.bestSpotToUSD(ctx, addr, ts)
}

func (r *DBRouter) bestSpotToUSD(ctx context.Context, token string, ts time.Time) (string, bool) {
	addr := strings.ToLower(token)
	wzilAddr := strings.ToLower(r.wzil)
	wzilDecimals := r.getDecimals(ctx, wzilAddr)
	zilUsd, ok := r.zilProvider.PriceZILUSD(ctx, ts)
	if !ok {
		return "", false
	}

	type candidate struct {
		priceUSD       string
		liquidityWZIL  string
		counterAddress string
	}
	best := candidate{}
	found := false

	consider := func(priceUSD, liquidityWZIL, counter string) {
		if priceUSD == "" || liquidityWZIL == "" {
			return
		}
		if !sanitizePrice(priceUSD) {
			return
		}
		if !found || greaterThan(liquidityWZIL, best.liquidityWZIL) {
			best = candidate{priceUSD: priceUSD, liquidityWZIL: liquidityWZIL, counterAddress: counter}
			found = true
		}
	}

	// V2 candidates (token with WZIL or stablecoin)
	q := `
		SELECT token0, token1, reserve0::numeric, reserve1::numeric
		FROM uniswap_v2_pairs
		WHERE (token0 = $1 AND (token1 = $2 OR token1 = ANY($3)))
		   OR (token1 = $1 AND (token0 = $2 OR token0 = ANY($3)))`
	if rows, err := r.pool.Query(ctx, q, addr, wzilAddr, r.stableList()); err == nil {
		defer rows.Close()
		for rows.Next() {
			var t0, t1 string
			var r0, r1 sql.NullString
			if err := rows.Scan(&t0, &t1, &r0, &r1); err != nil {
				continue
			}
			if !r0.Valid || !r1.Valid {
				continue
			}
			t0 = strings.ToLower(t0)
			t1 = strings.ToLower(t1)
			if t0 != addr && t1 != addr {
				continue
			}
			dec0 := r.getDecimals(ctx, t0)
			dec1 := r.getDecimals(ctx, t1)
			var priceInCounter, counterReserve string
			var counterDecimals int
			var counter string
			if t0 == addr {
				counter = t1
				counterReserve = r1.String
				counterDecimals = dec1
				pBase, ok := divStrings(r1.String, r0.String)
				if !ok {
					continue
				}
				scale := pow10BigRat(dec0 - dec1)
				p := new(big.Rat)
				if _, ok := p.SetString(pBase); !ok {
					continue
				}
				p.Mul(p, scale)
				priceInCounter = p.FloatString(18)
			} else {
				counter = t0
				counterReserve = r0.String
				counterDecimals = dec0
				pBase, ok := divStrings(r0.String, r1.String)
				if !ok {
					continue
				}
				scale := pow10BigRat(dec1 - dec0)
				p := new(big.Rat)
				if _, ok := p.SetString(pBase); !ok {
					continue
				}
				p.Mul(p, scale)
				priceInCounter = p.FloatString(18)
			}
			minReserve := minReserveForDecimals(counterDecimals)
			if lessThan(counterReserve, minReserve) {
				continue
			}
			if counter == wzilAddr {
				liq, ok := baseToHuman(counterReserve, wzilDecimals)
				if !ok {
					continue
				}
				priceUSD, ok := mulStrings(priceInCounter, zilUsd)
				if !ok {
					continue
				}
				consider(priceUSD, liq, counter)
				continue
			}
			if r.stable[counter] {
				priceInWZIL, _, ok := r.stablecoinPriceInWZIL(ctx, counter)
				if !ok {
					continue
				}
				stableUSD, ok := mulStrings(priceInWZIL, zilUsd)
				if !ok {
					continue
				}
				priceUSD, ok := mulStrings(priceInCounter, stableUSD)
				if !ok {
					continue
				}
				stableResHuman, ok := baseToHuman(counterReserve, counterDecimals)
				if !ok {
					continue
				}
				liqWZIL, ok := mulStrings(stableResHuman, priceInWZIL)
				if !ok {
					continue
				}
				consider(priceUSD, liqWZIL, counter)
			}
		}
	}

	// V3 candidates (token with WZIL or stablecoin)
	q3 := `
		SELECT token0, token1, sqrt_price_x96::numeric, reserve0::numeric, reserve1::numeric
		FROM uniswap_v3_pools
		WHERE (token0 = $1 AND (token1 = $2 OR token1 = ANY($3)))
		   OR (token1 = $1 AND (token0 = $2 OR token0 = ANY($3)))`
	if rows, err := r.pool.Query(ctx, q3, addr, wzilAddr, r.stableList()); err == nil {
		defer rows.Close()
		for rows.Next() {
			var t0, t1 string
			var sp, r0, r1 sql.NullString
			if err := rows.Scan(&t0, &t1, &sp, &r0, &r1); err != nil {
				continue
			}
			if !sp.Valid || !r0.Valid || !r1.Valid {
				continue
			}
			if !validSqrtX96(sp.String) {
				continue
			}
			t0 = strings.ToLower(t0)
			t1 = strings.ToLower(t1)
			if t0 != addr && t1 != addr {
				continue
			}
			dec0 := r.getDecimals(ctx, t0)
			dec1 := r.getDecimals(ctx, t1)
			price0, price1, ok := pricesFromSqrtX96(sp.String, dec0, dec1)
			if !ok {
				continue
			}
			var priceInCounter, counterReserve string
			var counterDecimals int
			var counter string
			if t0 == addr {
				counter = t1
				counterReserve = r1.String
				counterDecimals = dec1
				priceInCounter = price0
			} else {
				counter = t0
				counterReserve = r0.String
				counterDecimals = dec0
				priceInCounter = price1
			}
			minReserve := minReserveForDecimals(counterDecimals)
			if lessThan(counterReserve, minReserve) {
				continue
			}
			if counter == wzilAddr {
				liq, ok := baseToHuman(counterReserve, wzilDecimals)
				if !ok {
					continue
				}
				priceUSD, ok := mulStrings(priceInCounter, zilUsd)
				if !ok {
					continue
				}
				consider(priceUSD, liq, counter)
				continue
			}
			if r.stable[counter] {
				priceInWZIL, _, ok := r.stablecoinPriceInWZIL(ctx, counter)
				if !ok {
					continue
				}
				stableUSD, ok := mulStrings(priceInWZIL, zilUsd)
				if !ok {
					continue
				}
				priceUSD, ok := mulStrings(priceInCounter, stableUSD)
				if !ok {
					continue
				}
				stableResHuman, ok := baseToHuman(counterReserve, counterDecimals)
				if !ok {
					continue
				}
				liqWZIL, ok := mulStrings(stableResHuman, priceInWZIL)
				if !ok {
					continue
				}
				consider(priceUSD, liqWZIL, counter)
			}
		}
	}

	if !found {
		return "", false
	}
	return best.priceUSD, true
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
		if !ok {
			return "", false
		}
		// scale by 10^(d0-d1)
		scale := pow10BigRat(d0 - d1)
		p := new(big.Rat)
		if _, ok := p.SetString(pBase); !ok {
			return "", false
		}
		p.Mul(p, scale)
		return r.toUSD(ctx, strings.ToLower(t1), ts, p.FloatString(18))
	}
	// token is token1: price(token1 in token0) = (r0/r1) * 10^(d1-d0)
	pBase, ok := divStrings(r0.String, r1.String)
	if !ok {
		return "", false
	}
	scale := pow10BigRat(d1 - d0)
	p := new(big.Rat)
	if _, ok := p.SetString(pBase); !ok {
		return "", false
	}
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
	if !sp.Valid || !r0.Valid || !r1.Valid {
		return "", false
	}
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
	if !ok {
		return "", false
	}
	if strings.ToLower(t0) == strings.ToLower(token) {
		return r.toUSD(ctx, strings.ToLower(t1), ts, price0)
	}
	return r.toUSD(ctx, strings.ToLower(t0), ts, price1)
}

// toUSD converts a price in counter token units into USD if counter is WZIL or stablecoin
func (r *DBRouter) toUSD(ctx context.Context, counter string, ts time.Time, priceInCounter string) (string, bool) {
	// counter is stablecoin => multiply by stablecoin's actual price (from WZIL pair)
	if r.stable[counter] {
		stableUsd, ok := r.stablecoinPriceFromWZIL(ctx, counter, ts)
		if !ok {
			return "", false
		}
		prod, ok := mulStrings(priceInCounter, stableUsd)
		return prod, ok
	}
	// counter is WZIL => multiply by ZIL price
	if counter == r.wzil {
		zilUsd, ok := r.zilProvider.PriceZILUSD(ctx, ts)
		if !ok {
			return "", false
		}
		prod, ok := mulStrings(priceInCounter, zilUsd)
		return prod, ok
	}
	return "", false
}

// stablecoinPriceFromWZIL derives a stablecoin's USD price from its WZIL pair only.
// This avoids circular dependencies from stablecoin/stablecoin pairs.
func (r *DBRouter) stablecoinPriceFromWZIL(ctx context.Context, stablecoin string, ts time.Time) (string, bool) {
	priceInWZIL, _, ok := r.stablecoinPriceInWZIL(ctx, stablecoin)
	if !ok {
		return "", false
	}
	zilUsd, ok := r.zilProvider.PriceZILUSD(ctx, ts)
	if !ok {
		return "", false
	}
	prod, ok := mulStrings(priceInWZIL, zilUsd)
	if ok && sanitizePrice(prod) {
		return prod, true
	}
	return "", false
}

func (r *DBRouter) stablecoinPriceInWZIL(ctx context.Context, stablecoin string) (string, string, bool) {
	type candidate struct {
		priceInWZIL string
		wzilReserve string
	}
	best := candidate{}
	found := false
	stableAddr := strings.ToLower(stablecoin)
	wzilAddr := strings.ToLower(r.wzil)

	// Consider all V2 pairs with WZIL
	q := `
		SELECT token0, token1, reserve0::numeric, reserve1::numeric
		FROM uniswap_v2_pairs
		WHERE (token0 = $1 AND token1 = $2)
		   OR (token1 = $1 AND token0 = $2)`
	if rows, err := r.pool.Query(ctx, q, stableAddr, wzilAddr); err == nil {
		defer rows.Close()
		for rows.Next() {
			var t0, t1 string
			var r0, r1 sql.NullString
			if err := rows.Scan(&t0, &t1, &r0, &r1); err != nil {
				continue
			}
			if !r0.Valid || !r1.Valid {
				continue
			}
			priceInWZIL, wzilReserve, ok := r.v2StablePriceInWZIL(ctx, stableAddr, wzilAddr, t0, t1, r0.String, r1.String)
			if !ok {
				continue
			}
			if !found || greaterThan(wzilReserve, best.wzilReserve) {
				best = candidate{priceInWZIL: priceInWZIL, wzilReserve: wzilReserve}
				found = true
			}
		}
	}

	// Consider all V3 pools with WZIL
	q3 := `
		SELECT token0, token1, sqrt_price_x96::numeric, reserve0::numeric, reserve1::numeric
		FROM uniswap_v3_pools
		WHERE (token0 = $1 AND token1 = $2)
		   OR (token1 = $1 AND token0 = $2)`
	if rows, err := r.pool.Query(ctx, q3, stableAddr, wzilAddr); err == nil {
		defer rows.Close()
		for rows.Next() {
			var t0, t1 string
			var sp, r0, r1 sql.NullString
			if err := rows.Scan(&t0, &t1, &sp, &r0, &r1); err != nil {
				continue
			}
			if !sp.Valid || !r0.Valid || !r1.Valid {
				continue
			}
			priceInWZIL, wzilReserve, ok := r.v3StablePriceInWZIL(ctx, stableAddr, wzilAddr, t0, t1, sp.String, r0.String, r1.String)
			if !ok {
				continue
			}
			if !found || greaterThan(wzilReserve, best.wzilReserve) {
				best = candidate{priceInWZIL: priceInWZIL, wzilReserve: wzilReserve}
				found = true
			}
		}
	}

	if !found {
		return "", "", false
	}
	return best.priceInWZIL, best.wzilReserve, true
}

func (r *DBRouter) v2StablePriceInWZIL(ctx context.Context, stable, wzil, t0, t1, r0, r1 string) (string, string, bool) {
	stable = strings.ToLower(stable)
	t0 = strings.ToLower(t0)
	t1 = strings.ToLower(t1)
	if t0 != stable && t1 != stable {
		return "", "", false
	}
	if t0 != wzil && t1 != wzil {
		return "", "", false
	}
	var rStable, rWZIL string
	var dStable, dWZIL int
	if t0 == stable {
		rStable = r0
		rWZIL = r1
		dStable = r.getDecimals(ctx, t0)
		dWZIL = r.getDecimals(ctx, t1)
	} else {
		rStable = r1
		rWZIL = r0
		dStable = r.getDecimals(ctx, t1)
		dWZIL = r.getDecimals(ctx, t0)
	}
	if lessThan(rStable, "1") || lessThan(rWZIL, "1") {
		return "", "", false
	}
	pBase, ok := divStrings(rWZIL, rStable)
	if !ok {
		return "", "", false
	}
	scale := pow10BigRat(dStable - dWZIL)
	p := new(big.Rat)
	if _, ok := p.SetString(pBase); !ok {
		return "", "", false
	}
	p.Mul(p, scale)
	return p.FloatString(18), rWZIL, true
}

func (r *DBRouter) v3StablePriceInWZIL(ctx context.Context, stable, wzil, t0, t1, sqrtPrice, r0, r1 string) (string, string, bool) {
	if !validSqrtX96(sqrtPrice) {
		return "", "", false
	}
	stable = strings.ToLower(stable)
	t0 = strings.ToLower(t0)
	t1 = strings.ToLower(t1)
	if t0 != stable && t1 != stable {
		return "", "", false
	}
	if t0 != wzil && t1 != wzil {
		return "", "", false
	}
	if t0 == stable && lessThan(r1, "1") {
		return "", "", false
	}
	if t1 == stable && lessThan(r0, "1") {
		return "", "", false
	}
	dec0 := r.getDecimals(ctx, t0)
	dec1 := r.getDecimals(ctx, t1)
	price0, price1, ok := pricesFromSqrtX96(sqrtPrice, dec0, dec1)
	if !ok {
		return "", "", false
	}
	if t0 == stable {
		return price0, r1, true
	}
	return price1, r0, true
}

func (r *DBRouter) stableList() []string {
	lst := make([]string, 0, len(r.stable))
	for a := range r.stable {
		lst = append(lst, a)
	}
	return lst
}

func (r *DBRouter) getDecimals(ctx context.Context, token string) int {
	addr := strings.ToLower(token)
	r.muDecimals.RLock()
	if d, ok := r.decimals[addr]; ok {
		r.muDecimals.RUnlock()
		return d
	}
	r.muDecimals.RUnlock()
	var d sql.NullInt32
	_ = r.pool.QueryRow(ctx, `SELECT decimals FROM tokens WHERE address = $1`, addr).Scan(&d)
	val := 18
	if d.Valid {
		val = int(d.Int32)
	}
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
	if !ok {
		return "", "", false
	}
	num := new(big.Int).Mul(sqrt, sqrt)         // sqrt^2
	den := new(big.Int).Lsh(big.NewInt(1), 192) // 2^192
	ratio := new(big.Rat).SetFrac(num, den)
	// scale by decimals (normalize to human units)
	pow10 := pow10BigRat(dec0 - dec1)
	ratio.Mul(ratio, pow10)
	price0 = ratio.FloatString(18)
	// price1 is inverse adjusted: 1/price0
	if ratio.Sign() == 0 {
		return "", "", false
	}
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
	if _, ok := x.SetString(a); !ok {
		return "", false
	}
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok {
		return "", false
	}
	if y.Sign() == 0 {
		return "", false
	}
	x.Quo(x, y)
	return x.FloatString(18), true
}

func mulStrings(a, b string) (string, bool) {
	x := new(big.Rat)
	if _, ok := x.SetString(a); !ok {
		return "", false
	}
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok {
		return "", false
	}
	x.Mul(x, y)
	return x.FloatString(18), true
}

func baseToHuman(val string, decimals int) (string, bool) {
	x := new(big.Rat)
	if _, ok := x.SetString(val); !ok {
		return "", false
	}
	scale := pow10BigRat(decimals)
	if scale.Sign() == 0 {
		return "", false
	}
	x.Quo(x, scale)
	return x.FloatString(18), true
}

func lessThan(a, b string) bool {
	x := new(big.Rat)
	if _, ok := x.SetString(a); !ok {
		return false
	}
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok {
		return false
	}
	return x.Cmp(y) < 0
}

func greaterThan(a, b string) bool {
	x := new(big.Rat)
	if _, ok := x.SetString(a); !ok {
		return false
	}
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok {
		return false
	}
	return x.Cmp(y) > 0
}

func validSqrtX96(s string) bool {
	v := new(big.Int)
	if _, ok := v.SetString(s, 10); !ok {
		return false
	}
	if v.Sign() <= 0 {
		return false
	}
	min := big.NewInt(1_000_000)
	max := new(big.Int)
	_, _ = max.SetString("100000000000000000000000000000000000000", 10) // 1e38
	if v.Cmp(min) < 0 {
		return false
	}
	if v.Cmp(max) > 0 {
		return false
	}
	return true
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
	if _, ok := p.SetString(price); !ok {
		return false
	}
	max := new(big.Rat)
	if _, ok := max.SetString(maxPrice); !ok {
		return false
	}
	return p.Cmp(max) <= 0 && p.Sign() >= 0
}
