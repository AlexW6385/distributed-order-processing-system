package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// --- scenario: flash sale ---------------------------------------------------

// flashsale releases o.orders single-unit orders from distinct customers at
// once against o.units of one product, with or without the Redis gate, and
// reports latency and how many transactions reached product-service's
// database.
func flashsale(ctx context.Context, o options) bool {
	products := mustPool(ctx, o.productsDB)
	defer products.Close()
	const product = "prod-headphones-014"

	mode := "Redis gate"
	if o.gate {
		code, _, _, err := call(http.MethodPost, o.base+"/admin/flash-sales", "", map[string]any{"product_id": product, "stock": o.units})
		if err != nil || code != http.StatusOK {
			return check(false, "start flash sale: %d %v", code, err)
		}
	} else {
		mode = "Postgres row lock only"
		rdb := redis.NewClient(&redis.Options{Addr: o.redisAddr})
		_ = rdb.Del(ctx, "flash:{"+product+"}:tokens", "flash:{"+product+"}:buyers", "flash:{"+product+"}:orders").Err()
		rdb.Close()
		if _, err := products.Exec(ctx, `UPDATE products SET stock = $2 WHERE id = $1`, product, o.units); err != nil {
			return check(false, "set stock: %v", err)
		}
	}
	var reservedBefore int
	_ = products.QueryRow(ctx, reservedQty, product).Scan(&reservedBefore)
	transactions := func() int64 {
		time.Sleep(11 * time.Second) // backends flush statistics within ~10s of going idle
		var n int64
		_ = products.QueryRow(ctx, `SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = current_database()`).Scan(&n)
		return n
	}
	txBefore := transactions()

	fmt.Printf("flashsale (%s): %d buyers for %d units of %s\n", mode, o.orders, o.units, product)
	rec := newRecorder()
	var created atomic.Int64
	gate := make(chan struct{})
	var wg sync.WaitGroup
	run := time.Now().UnixNano()
	for i := 0; i < o.orders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			code, _, d, err := createOrderFor(o.base, fmt.Sprintf("buyer-%d-%d@example.com", run, i), product, 1)
			rec.add("create", code, err, d)
			if code == http.StatusCreated {
				created.Add(1)
			}
		}(i)
	}
	start := time.Now()
	close(gate)
	wg.Wait()
	elapsed := time.Since(start)
	txAfter := transactions()

	fmt.Printf("  all responses in %.2fs (%.0f req/s)\n", elapsed.Seconds(), float64(o.orders)/elapsed.Seconds())
	rec.report()
	fmt.Printf("  product database transactions: %d (%.2f per request)\n", txAfter-txBefore, float64(txAfter-txBefore)/float64(o.orders))

	var stock, reserved int
	_ = products.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1`, product).Scan(&stock)
	_ = products.QueryRow(ctx, reservedQty, product).Scan(&reserved)
	reserved -= reservedBefore
	ok := check(created.Load() == int64(o.units), "sold exactly %d units (%d)", o.units, created.Load())
	ok = check(stock == 0 && int64(reserved) == created.Load(), "stock %d, reserved %d: nothing oversold", stock, reserved) && ok
	return ok
}

// --- scenario: chaos --------------------------------------------------------

// chaos runs a mixed workload (pay, refund, cancel) while Toxiproxy breaks
// the links from order-service to its dependencies, then removes the faults
// and verifies every invariant once the system has recovered.
//
// Timeline (fractions of -d):
//
//	10%-35%  payment link adds 3 s latency: charges time out with unknown outcomes
//	40%-60%  product link resets connections: retries, then the circuit breaker
//	65%-80%  payment link is down: the payment breaker opens and fails fast
func chaos(ctx context.Context, o options) bool {
	if err := toxiproxyReset(o.toxiproxy); err != nil {
		return check(false, "reach Toxiproxy at %s: %v", o.toxiproxy, err)
	}
	products := mustPool(ctx, o.productsDB)
	_, _ = products.Exec(ctx, `UPDATE products SET stock = 1000000`) // measure faults, not sell-outs
	products.Close()
	fmt.Printf("chaos: %d shoppers for %s\n", o.concurrency, o.duration)

	rec := newRecorder()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			shop(o.base, rec, stop, w)
		}(w)
	}

	at := func(frac float64) time.Duration { return time.Duration(frac * float64(o.duration)) }
	steps := []struct {
		at   time.Duration
		name string
		do   func() error
	}{
		{at(0.10), "payment: +3s latency", func() error {
			return toxiproxyToxic(o.toxiproxy, "payment", "slow", "latency", map[string]any{"latency": 3000})
		}},
		{at(0.35), "payment: latency removed", func() error { return toxiproxyRemove(o.toxiproxy, "payment", "slow") }},
		{at(0.40), "product: connection resets", func() error {
			return toxiproxyToxic(o.toxiproxy, "product", "reset", "reset_peer", map[string]any{"timeout": 0})
		}},
		{at(0.60), "product: resets removed", func() error { return toxiproxyRemove(o.toxiproxy, "product", "reset") }},
		{at(0.65), "payment: link down", func() error { return toxiproxyEnable(o.toxiproxy, "payment", false) }},
		{at(0.80), "payment: link restored", func() error { return toxiproxyEnable(o.toxiproxy, "payment", true) }},
	}
	begin := time.Now()
	for _, s := range steps {
		time.Sleep(time.Until(begin.Add(s.at)))
		err := s.do()
		fmt.Printf("  %5.1fs %s%s\n", time.Since(begin).Seconds(), s.name, errSuffix(err))
	}
	time.Sleep(time.Until(begin.Add(o.duration)))
	close(stop)
	wg.Wait()
	_ = toxiproxyReset(o.toxiproxy)

	rec.report()
	return verify(ctx, o)
}

// shop loops through a mixed workload until stop closes: most orders are
// paid, some cancelled, and some paid orders refunded.
func shop(base string, rec *recorder, stop <-chan struct{}, worker int) {
	products := []string{"prod-coffee-001", "prod-tea-002", "prod-mug-003", "prod-bottle-004", "prod-notebook-005", "prod-pen-006"}
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		default:
		}
		code, created, d, err := createOrderFor(base, fmt.Sprintf("shopper-%d@example.com", worker), products[rand.IntN(len(products))], 1)
		rec.add("create", code, err, d)
		if code != http.StatusCreated {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if i%10 == 9 {
			code, _, d, err := call(http.MethodPost, base+"/orders/"+created.ID+"/cancel", "", nil)
			rec.add("cancel", code, err, d)
			continue
		}
		code, paid, d, err := payOrder(base, created.ID)
		rec.add("pay", code, err, d)
		if code == http.StatusOK && paid.Status == "paid" && i%5 == 0 {
			code, _, d, err := call(http.MethodPost, base+"/orders/"+created.ID+"/refund", "", nil)
			rec.add("refund", code, err, d)
		}
	}
}

func toxiproxyReset(api string) error { return toxiproxyDo(http.MethodPost, api+"/reset", nil) }

func toxiproxyToxic(api, proxy, name, kind string, attrs map[string]any) error {
	return toxiproxyDo(http.MethodPost, api+"/proxies/"+proxy+"/toxics", map[string]any{
		"name": name, "type": kind, "stream": "downstream", "toxicity": 1.0, "attributes": attrs,
	})
}

func toxiproxyRemove(api, proxy, name string) error {
	return toxiproxyDo(http.MethodDelete, api+"/proxies/"+proxy+"/toxics/"+name, nil)
}

func toxiproxyEnable(api, proxy string, enabled bool) error {
	return toxiproxyDo(http.MethodPost, api+"/proxies/"+proxy, map[string]any{"enabled": enabled})
}

func toxiproxyDo(method, url string, body any) error {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, url, &buf)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("toxiproxy %s %s: %s", method, url, resp.Status)
	}
	return nil
}
