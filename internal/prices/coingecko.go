package prices

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	coingeckoAPIURL     = "https://api.coingecko.com/api/v3/simple/price"
	coingeckoTimeout    = 10 * time.Second
	coingeckoRetryDelay = 5 * time.Second
	coingeckoMaxRetries = 3
)

type CoinGeckoClient struct {
	httpClient *http.Client
}

func NewCoinGeckoClient() *CoinGeckoClient {
	return &CoinGeckoClient{
		httpClient: &http.Client{
			Timeout: coingeckoTimeout,
		},
	}
}

type coingeckoResponse struct {
	Zilliqa struct {
		USD float64 `json:"usd"`
	} `json:"zilliqa"`
}

func (c *CoinGeckoClient) FetchZILUSD(ctx context.Context) (float64, error) {
	url := fmt.Sprintf("%s?ids=zilliqa&vs_currencies=usd", coingeckoAPIURL)

	var lastErr error
	for attempt := 0; attempt < coingeckoMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(coingeckoRetryDelay):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return 0, fmt.Errorf("create request to %s: %w", url, err)
		}

		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http request failed (attempt %d/%d): %w", attempt+1, coingeckoMaxRetries, err)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			lastErr = fmt.Errorf("rate limited (attempt %d/%d)", attempt+1, coingeckoMaxRetries)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("unexpected status %d (attempt %d/%d): %s", resp.StatusCode, attempt+1, coingeckoMaxRetries, string(body))
			continue
		}

		var result coingeckoResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return 0, fmt.Errorf("decode response: %w", err)
		}
		resp.Body.Close()

		if result.Zilliqa.USD == 0 {
			return 0, fmt.Errorf("received zero price from CoinGecko")
		}

		return result.Zilliqa.USD, nil
	}

	return 0, fmt.Errorf("failed after %d attempts: %w", coingeckoMaxRetries, lastErr)
}
