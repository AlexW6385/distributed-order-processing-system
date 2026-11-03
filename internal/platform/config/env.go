package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func String(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func CSV(key, fallback string) []string {
	raw := String(key, fallback)
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// Duration parses a Go duration such as "15m" or "500ms".
func Duration(key string, fallback time.Duration) time.Duration {
	raw := String(key, "")
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		panic(fmt.Sprintf("config %s: %v", key, err))
	}
	return d
}

func Int(key string, fallback int) int {
	raw := String(key, "")
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		panic(fmt.Sprintf("config %s: %v", key, err))
	}
	return n
}

func Float(key string, fallback float64) float64 {
	raw := String(key, "")
	if raw == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		panic(fmt.Sprintf("config %s: %v", key, err))
	}
	return f
}
