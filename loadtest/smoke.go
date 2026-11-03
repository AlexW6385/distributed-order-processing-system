package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// smoke exercises every API path once and checks the behaviour a client sees,
// including effects that arrive asynchronously through Kafka.
func smoke(ctx context.Context, o options) bool {
	fmt.Printf("smoke: %s\n", o.base)
	ok := true
	run := func(name string, fn func() error) {
		err := fn()
		ok = check(err == nil, "%s%s", name, errSuffix(err)) && ok
	}
	if !waitFor(60*time.Second, func() bool { code, _ := get(o.base + "/products"); return code == http.StatusOK }) {
		return check(false, "gateway answers /products")
	}

	run("create and pay an order", func() error {
		before := stockOf(o.base, "prod-coffee-001")
		created := mustCreate(o.base, "smoke@example.com", "prod-coffee-001", 2)
		if created.Status != "stock_reserved" {
			return fmt.Errorf("status %s", created.Status)
		}
		// The catalog is cached; invalidation makes the change visible quickly.
		if !waitFor(10*time.Second, func() bool { return stockOf(o.base, "prod-coffee-001") == before-2 }) {
			return fmt.Errorf("stock did not drop by 2 from %d", before)
		}
		code, paid, _, _ := payOrder(o.base, created.ID)
		if code != http.StatusOK || paid.Status != "paid" {
			return fmt.Errorf("pay: %d %s", code, paid.Status)
		}
		code, again, _, _ := call(http.MethodPost, o.base+"/orders/"+created.ID+"/pay", "different-key", nil)
		if code != http.StatusOK || again.Status != "paid" {
			return fmt.Errorf("repeat pay: %d %s", code, again.Status)
		}
		return nil
	})

	run("10 concurrent pays end in one paid order", func() error {
		created := mustCreate(o.base, "smoke@example.com", "prod-mug-003", 1)
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _, _, _ = call(http.MethodPost, o.base+"/orders/"+created.ID+"/pay", fmt.Sprintf("race-%d", i), nil)
			}(i)
		}
		wg.Wait()
		if s := statusOf(o.base, created.ID); s != "paid" {
			return fmt.Errorf("status %s", s)
		}
		return nil
	})

	run("cancel releases stock through order.cancelled", func() error {
		before := stockOf(o.base, "prod-tea-002")
		created := mustCreate(o.base, "smoke@example.com", "prod-tea-002", 3)
		code, cancelled, _, _ := call(http.MethodPost, o.base+"/orders/"+created.ID+"/cancel", "", nil)
		if code != http.StatusOK || cancelled.Status != "cancelled" {
			return fmt.Errorf("cancel: %d %s", code, cancelled.Status)
		}
		if !waitFor(30*time.Second, func() bool { return stockOf(o.base, "prod-tea-002") == before }) {
			return fmt.Errorf("stock not released")
		}
		if code, _, _, _ := payOrder(o.base, created.ID); code != http.StatusConflict {
			return fmt.Errorf("paying a cancelled order: %d, want 409", code)
		}
		return nil
	})

	run("refund returns money and stock through order.refunded", func() error {
		before := stockOf(o.base, "prod-notebook-005")
		created := mustCreate(o.base, "smoke@example.com", "prod-notebook-005", 2)
		if code, _, _, _ := payOrder(o.base, created.ID); code != http.StatusOK {
			return fmt.Errorf("pay: %d", code)
		}
		code, refunded, _, _ := call(http.MethodPost, o.base+"/orders/"+created.ID+"/refund", "", nil)
		if code != http.StatusOK || refunded.Status != "refunded" {
			return fmt.Errorf("refund: %d %s", code, refunded.Status)
		}
		if !waitFor(30*time.Second, func() bool { return stockOf(o.base, "prod-notebook-005") == before }) {
			return fmt.Errorf("stock not returned")
		}
		if code, again, _, _ := call(http.MethodPost, o.base+"/orders/"+created.ID+"/refund", "", nil); code != http.StatusOK || again.Status != "refunded" {
			return fmt.Errorf("repeat refund: %d %s", code, again.Status)
		}
		return nil
	})

	run("large payment is declined and stock released", func() error {
		before := stockOf(o.base, "prod-keyboard-007")
		created := mustCreate(o.base, "smoke@example.com", "prod-keyboard-007", 17) // above the decline limit
		if _, paid, _, _ := payOrder(o.base, created.ID); paid.Status != "failed" {
			return fmt.Errorf("status %s, want failed", paid.Status)
		}
		if !waitFor(30*time.Second, func() bool { return stockOf(o.base, "prod-keyboard-007") == before }) {
			return fmt.Errorf("stock not released")
		}
		return nil
	})

	run("flash sale: one unit per customer, sold out beyond stock", func() error {
		code, _, _, _ := call(http.MethodPost, o.base+"/admin/flash-sales", "", map[string]any{"product_id": "prod-speaker-015", "stock": 2})
		if code != http.StatusOK {
			return fmt.Errorf("start sale: %d", code)
		}
		run := time.Now().UnixNano()
		if c, _, _, _ := createOrderFor(o.base, fmt.Sprintf("a%d@x.co", run), "prod-speaker-015", 1); c != http.StatusCreated {
			return fmt.Errorf("first buyer: %d", c)
		}
		if c, _, _, _ := createOrderFor(o.base, fmt.Sprintf("a%d@x.co", run), "prod-speaker-015", 1); c != http.StatusConflict {
			return fmt.Errorf("same customer again: %d, want 409", c)
		}
		if c, _, _, _ := createOrderFor(o.base, fmt.Sprintf("b%d@x.co", run), "prod-speaker-015", 1); c != http.StatusCreated {
			return fmt.Errorf("second buyer: %d", c)
		}
		if c, _, _, _ := createOrderFor(o.base, fmt.Sprintf("c%d@x.co", run), "prod-speaker-015", 1); c != http.StatusConflict {
			return fmt.Errorf("third buyer: %d, want 409 sold out", c)
		}
		return nil
	})

	run("errors map to HTTP status codes", func() error {
		if c, _, _, _ := createOrder(o.base, "prod-lamp-009", 100000); c != http.StatusConflict {
			return fmt.Errorf("insufficient stock: %d, want 409", c)
		}
		if c, _, _, _ := call(http.MethodPost, o.base+"/orders", "", map[string]any{"customer_email": "nope", "items": []any{}}); c != http.StatusBadRequest {
			return fmt.Errorf("invalid input: %d, want 400", c)
		}
		if c, _ := get(o.base + "/orders/does-not-exist"); c != http.StatusNotFound {
			return fmt.Errorf("unknown order: %d, want 404", c)
		}
		return nil
	})

	run("create with the same Idempotency-Key returns the same order", func() error {
		key := fmt.Sprintf("create-%d", time.Now().UnixNano())
		body := map[string]any{"customer_email": "smoke@example.com", "items": []map[string]any{{"product_id": "prod-pen-006", "quantity": 1}}}
		_, first, _, _ := call(http.MethodPost, o.base+"/orders", key, body)
		_, second, _, _ := call(http.MethodPost, o.base+"/orders", key, body)
		if first.ID == "" || first.ID != second.ID {
			return fmt.Errorf("got %q and %q", first.ID, second.ID)
		}
		return nil
	})
	return ok
}

func mustCreate(base, email, product string, qty int) order {
	_, created, _, _ := createOrderFor(base, email, product, qty)
	return created
}

func get(url string) (int, []byte) {
	resp, err := client.Get(url)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	buf, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, buf
}

func stockOf(base, productID string) int {
	_, body := get(base + "/products")
	var out struct {
		Products []struct {
			ID    string `json:"id"`
			Stock int    `json:"stock"`
		} `json:"products"`
	}
	_ = json.Unmarshal(body, &out)
	for _, p := range out.Products {
		if p.ID == productID {
			return p.Stock
		}
	}
	return -1
}

func statusOf(base, orderID string) string {
	_, body := get(base + "/orders/" + orderID)
	var out struct {
		Order order `json:"order"`
	}
	_ = json.Unmarshal(body, &out)
	return out.Order.Status
}

func waitFor(limit time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(limit); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return false
}

func errSuffix(err error) string {
	if err == nil {
		return ""
	}
	return ": " + err.Error()
}
