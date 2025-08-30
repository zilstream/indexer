package database

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestHashToString(t *testing.T) {
	hash := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	result := HashToString(hash)
	expected := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	
	if result != expected {
		t.Errorf("HashToString() = %v, want %v", result, expected)
	}
}

func TestAddressToString(t *testing.T) {
	addr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb3")
	result := AddressToString(addr)
	// Ethereum addresses are checksummed, so the result should match the checksummed format
	expected := "0x742D35cC6634C0532925a3B844Bc9e7595F0bEb3"
	
	if result != expected {
		t.Errorf("AddressToString() = %v, want %v", result, expected)
	}
}

func TestBigIntToNumeric(t *testing.T) {
	tests := []struct {
		name  string
		value *big.Int
		want  *string
	}{
		{
			name:  "nil value",
			value: nil,
			want:  nil,
		},
		{
			name:  "zero value",
			value: big.NewInt(0),
			want:  stringPtr("0"),
		},
		{
			name:  "positive value",
			value: big.NewInt(1000000),
			want:  stringPtr("1000000"),
		},
		{
			name:  "large value",
			value: new(big.Int).SetBytes([]byte{255, 255, 255, 255, 255, 255, 255, 255}),
			want:  stringPtr("18446744073709551615"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BigIntToNumeric(tt.value)
			if (result == nil) != (tt.want == nil) {
				t.Errorf("BigIntToNumeric() = %v, want %v", result, tt.want)
				return
			}
			if result != nil && *result != *tt.want {
				t.Errorf("BigIntToNumeric() = %v, want %v", *result, *tt.want)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}