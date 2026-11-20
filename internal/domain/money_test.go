package domain

import "testing"

func TestParseAmount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		currency Currency
		want     int64
		wantErr  bool
	}{
		{"usd whole", "10", "USD", 1000, false},
		{"usd decimals", "10.50", "USD", 1050, false},
		{"usd leading dot", ".5", "USD", 50, false},
		{"usd zero", "0.00", "USD", 0, false},
		{"usd negative", "-1.25", "USD", -125, false},
		{"jpy scale zero", "100", "JPY", 100, false},
		{"jpy rejects decimals", "100.5", "JPY", 0, true},
		{"kwd scale three", "1.234", "KWD", 1234, false},
		{"too many decimals", "1.234", "USD", 0, true},
		{"empty", "", "USD", 0, true},
		{"not a number", "1,00", "USD", 0, true},
		{"exponent", "1e3", "USD", 0, true},
		{"double dot", "1.2.3", "USD", 0, true},
		{"overflow", "9999999999999999999", "USD", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseAmount(tt.input, tt.currency)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseAmount() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("ParseAmount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFormatAmount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		minor    int64
		currency Currency
		want     string
	}{
		{1050, "USD", "10.50"},
		{5, "USD", "0.05"},
		{-125, "USD", "-1.25"},
		{100, "JPY", "100"},
		{1234, "KWD", "1.234"},
	}
	for _, tt := range tests {
		if got := FormatAmount(tt.minor, tt.currency); got != tt.want {
			t.Errorf("FormatAmount(%d, %s) = %q, want %q", tt.minor, tt.currency, got, tt.want)
		}
	}
}

func TestParseCurrency(t *testing.T) {
	t.Parallel()
	if _, err := ParseCurrency("usd"); err != nil {
		t.Fatalf("lowercase currency should be accepted: %v", err)
	}
	for _, invalid := range []string{"", "US", "USDX", "U$D"} {
		if _, err := ParseCurrency(invalid); err == nil {
			t.Fatalf("expected %q to be rejected", invalid)
		}
	}
}
