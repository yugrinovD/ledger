package domain

import (
	"fmt"
	"math"
	"strings"
)

type Currency string

func ParseCurrency(v string) (Currency, error) {
	v = strings.ToUpper(strings.TrimSpace(v))
	if len(v) != 3 {
		return "", fmt.Errorf("%w: currency must be a 3-letter ISO code", ErrValidation)
	}
	for _, r := range v {
		if r < 'A' || r > 'Z' {
			return "", fmt.Errorf("%w: currency must contain only A-Z", ErrValidation)
		}
	}
	return Currency(v), nil
}

func (c Currency) String() string { return string(c) }

// Scale returns the number of minor units per major unit for common ISO 4217
// currencies. The default is 2, which covers most of the world.
func (c Currency) Scale() int {
	switch string(c) {
	case "BIF", "CLP", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW", "PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF":
		return 0
	case "BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND":
		return 3
	default:
		return 2
	}
}

// ParseAmount parses a human-readable decimal amount into minor units. It
// deliberately rejects floating point, exponents and thousands separators: money
// must never pass through a binary float.
func ParseAmount(input string, currency Currency) (int64, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return 0, fmt.Errorf("%w: amount is required", ErrValidation)
	}
	negative := false
	switch s[0] {
	case '-':
		negative = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if s == "" {
		return 0, fmt.Errorf("%w: invalid amount", ErrValidation)
	}

	whole, frac, hasDot := strings.Cut(s, ".")
	if hasDot && strings.Contains(frac, ".") {
		return 0, fmt.Errorf("%w: invalid amount", ErrValidation)
	}
	if whole == "" {
		whole = "0"
	}
	scale := currency.Scale()
	if len(frac) > scale {
		return 0, fmt.Errorf("%w: amount has more than %d fractional digits for %s", ErrValidation, scale, currency)
	}
	frac += strings.Repeat("0", scale-len(frac))

	digits := whole + frac
	if len(digits) > 18 {
		return 0, fmt.Errorf("%w: amount is too large", ErrValidation)
	}
	var minor int64
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%w: amount must contain only digits", ErrValidation)
		}
		if minor > (math.MaxInt64-int64(r-'0'))/10 {
			return 0, fmt.Errorf("%w: amount is too large", ErrValidation)
		}
		minor = minor*10 + int64(r-'0')
	}
	if negative {
		minor = -minor
	}
	return minor, nil
}

// FormatAmount renders minor units using the currency scale.
func FormatAmount(minor int64, currency Currency) string {
	scale := currency.Scale()
	if scale == 0 {
		return fmt.Sprintf("%d", minor)
	}
	sign := ""
	u := minor
	if u < 0 {
		sign = "-"
		u = -u
	}
	div := int64(1)
	for i := 0; i < scale; i++ {
		div *= 10
	}
	whole := u / div
	frac := u % div
	return fmt.Sprintf("%s%d.%0*d", sign, whole, scale, frac)
}
