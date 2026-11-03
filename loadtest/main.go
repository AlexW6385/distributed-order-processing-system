// Command loadtest drives the public HTTP API and checks the system's
// invariants under load.
//
//	go run ./loadtest checkout -c 200 -d 60s
//	go run ./loadtest oversell -orders 1000 -units 100
//	go run ./loadtest faults   -orders 500            # payment-service needs PAYMENT_FAULT_RATE > 0
//
// The scenarios read service databases to set up fixtures and to verify
// results; pass their URLs with -products-db, -orders-db and -payments-db
// (defaults match docker-compose). Run the gateway with RATE_LIMIT_PER_MINUTE=0,
// otherwise the per-IP limiter rejects most of the load.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type options struct {
	base        string
	concurrency int
	duration    time.Duration
	warmup      time.Duration
	orders      int
	units       int
	productsDB  string
	ordersDB    string
	paymentsDB  string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: loadtest checkout|oversell|faults [flags]")
		os.Exit(2)
	}
	scenario := os.Args[1]
	fs := flag.NewFlagSet(scenario, flag.ExitOnError)
	var o options
	fs.StringVar(&o.base, "base", "http://localhost:8080", "gateway base URL")
	fs.IntVar(&o.concurrency, "c", 100, "concurrent virtual users")
	fs.DurationVar(&o.duration, "d", 30*time.Second, "measured duration (checkout)")
	fs.DurationVar(&o.warmup, "warmup", 5*time.Second, "unmeasured warm-up (checkout)")
	fs.IntVar(&o.orders, "orders", 1000, "orders to place (oversell, faults)")
	fs.IntVar(&o.units, "units", 100, "units of stock to compete for (oversell)")
	pg := "postgres://orders:orders@localhost:5432/"
	fs.StringVar(&o.productsDB, "products-db", pg+"products?sslmode=disable", "product-service database")
	fs.StringVar(&o.ordersDB, "orders-db", pg+"orders?sslmode=disable", "order-service database")
	fs.StringVar(&o.paymentsDB, "payments-db", pg+"payments?sslmode=disable", "payment-service database")
	_ = fs.Parse(os.Args[2:])

	ctx := context.Background()
	var ok bool
	switch scenario {
	case "checkout":
		ok = checkout(ctx, o)
	case "oversell":
		ok = oversell(ctx, o)
	case "faults":
		ok = faults(ctx, o)
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", scenario)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

// --- HTTP client ------------------------------------------------------------

var client = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 2000,
		IdleConnTimeout:     90 * time.Second,
	},
}

type order struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func call(method, url, key string, body any) (int, order, time.Duration, error) {
	var reader io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, url, reader)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		return 0, order{}, elapsed, err
	}
	defer resp.Body.Close()
	var out struct {
		Order order `json:"order"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out.Order, elapsed, nil
}

func createOrder(base, productID string, qty int) (int, order, time.Duration, error) {
	return call(http.MethodPost, base+"/orders", "", map[string]any{
		"customer_email": "load@example.com",
		"items":          []map[string]any{{"product_id": productID, "quantity": qty}},
	})
}

func payOrder(base, orderID string) (int, order, time.Duration, error) {
	return call(http.MethodPost, base+"/orders/"+orderID+"/pay", "pay-"+orderID, nil)
}

// --- stats ------------------------------------------------------------------

type recorder struct {
	mu        sync.Mutex
	latencies map[string][]time.Duration
	codes     map[string]int
}

func newRecorder() *recorder {
	return &recorder{latencies: map[string][]time.Duration{}, codes: map[string]int{}}
}

func (r *recorder) add(op string, code int, err error, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%s %d", op, code)
	if err != nil {
		key = op + " error"
	}
	r.codes[key]++
	r.latencies[op] = append(r.latencies[op], d)
}

func (r *recorder) report() {
	ops := make([]string, 0, len(r.latencies))
	for op := range r.latencies {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	fmt.Printf("  %-10s %8s %8s %8s %8s %8s\n", "latency", "count", "p50", "p95", "p99", "max")
	for _, op := range ops {
		l := r.latencies[op]
		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
		fmt.Printf("  %-10s %8d %8s %8s %8s %8s\n", op, len(l), ms(pct(l, 50)), ms(pct(l, 95)), ms(pct(l, 99)), ms(l[len(l)-1]))
	}
	codes := make([]string, 0, len(r.codes))
	for c := range r.codes {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	fmt.Print("  responses: ")
	for i, c := range codes {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%d", c, r.codes[c])
	}
	fmt.Println()
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := (len(sorted)*p + 99) / 100
	if i > 0 {
		i--
	}
	return sorted[i]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000) }

func mustPool(ctx context.Context, url string) *pgxpool.Pool {
	pool, err := pgxpool.New(ctx, url)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect %s: %v\n", url, err)
		os.Exit(1)
	}
	return pool
}

func check(ok bool, format string, args ...any) bool {
	mark := "PASS"
	if !ok {
		mark = "FAIL"
	}
	fmt.Printf("  [%s] %s\n", mark, fmt.Sprintf(format, args...))
	return ok
}

// --- scenario: checkout throughput ------------------------------------------

// checkout runs closed-loop virtual users that each create an order and pay
// it, as fast as the system answers.
func checkout(ctx context.Context, o options) bool {
	products := mustPool(ctx, o.productsDB)
	defer products.Close()
	orders := mustPool(ctx, o.ordersDB)
	defer orders.Close()

	// Plenty of stock so the run measures throughput, not sell-outs.
	if _, err := products.Exec(ctx, `UPDATE products SET stock = 100000000`); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return false
	}
	var ids []string
	rows, _ := products.Query(ctx, `SELECT id FROM products ORDER BY id`)
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()

	fmt.Printf("checkout: %d virtual users, %s warm-up, %s measured, %d products\n", o.concurrency, o.warmup, o.duration, len(ids))
	rec := newRecorder()
	var measuring atomic.Bool
	var paid, pending atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				code, created, d, err := createOrder(o.base, ids[rand.IntN(len(ids))], 1)
				on := measuring.Load()
				if on {
					rec.add("create", code, err, d)
				}
				if err != nil || code != http.StatusCreated {
					continue
				}
				code, result, d, err := payOrder(o.base, created.ID)
				if !on || !measuring.Load() {
					continue
				}
				rec.add("pay", code, err, d)
				if err == nil && code == http.StatusOK && result.Status == "paid" {
					paid.Add(1)
					rec.add("checkout", 200, nil, time.Since(start))
				} else if code == http.StatusAccepted {
					pending.Add(1)
				}
			}
		}()
	}
	time.Sleep(o.warmup)
	measuring.Store(true)
	began := time.Now()
	time.Sleep(o.duration)
	measuring.Store(false)
	elapsed := time.Since(began)
	close(stop)
	wg.Wait()

	fmt.Printf("  paid orders: %d in %.1fs = %.0f orders/s (%d left payment_pending)\n",
		paid.Load(), elapsed.Seconds(), float64(paid.Load())/elapsed.Seconds(), pending.Load())
	rec.report()

	// The outbox relays events asynchronously; report how long it takes to
	// drain after the load stops.
	drainStart := time.Now()
	var backlog int
	for {
		_ = orders.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE status = 'pending'`).Scan(&backlog)
		if backlog == 0 || time.Since(drainStart) > 60*time.Second {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("  outbox drained %.1fs after load stopped (backlog now %d)\n", time.Since(drainStart).Seconds(), backlog)

	var ok = true
	ok = check(rec.codes["create error"]+rec.codes["pay error"] == 0, "no transport errors") && ok
	ok = check(rec.codes["create 500"]+rec.codes["pay 500"] == 0, "no 500s") && ok
	ok = check(backlog == 0, "every event published") && ok
	return ok
}

// --- scenario: oversell -----------------------------------------------------

// oversell releases o.orders concurrent single-unit orders at once against a
// product with o.units in stock. Exactly o.units may succeed and stock must
// end at zero, never below.
func oversell(ctx context.Context, o options) bool {
	products := mustPool(ctx, o.productsDB)
	defer products.Close()
	const product = "prod-lamp-009"
	if _, err := products.Exec(ctx, `UPDATE products SET stock = $2 WHERE id = $1`, product, o.units); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return false
	}
	var reservedBefore int
	_ = products.QueryRow(ctx, reservedQty, product).Scan(&reservedBefore)

	fmt.Printf("oversell: %d concurrent orders for %d units of %s\n", o.orders, o.units, product)
	rec := newRecorder()
	var created atomic.Int64
	gate := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < o.orders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate // release every request at the same moment
			code, _, d, err := createOrder(o.base, product, 1)
			rec.add("create", code, err, d)
			if code == http.StatusCreated {
				created.Add(1)
			}
		}()
	}
	start := time.Now()
	close(gate)
	wg.Wait()
	fmt.Printf("  all responses in %.2fs\n", time.Since(start).Seconds())
	rec.report()

	// Orders that timed out are failed by order-service and their stock comes
	// back through order.failed; wait for that to settle.
	var stock, reserved int
	deadline := time.Now().Add(30 * time.Second)
	for {
		_ = products.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1`, product).Scan(&stock)
		_ = products.QueryRow(ctx, reservedQty, product).Scan(&reserved)
		reserved -= reservedBefore
		if int64(reserved) == created.Load() || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	ok := true
	ok = check(created.Load() <= int64(o.units), "accepted %d orders for %d units", created.Load(), o.units) && ok
	ok = check(stock >= 0 && stock+reserved == o.units, "stock %d + reserved %d = %d units (no oversell, no leak)", stock, reserved, o.units) && ok
	ok = check(int64(reserved) == created.Load(), "reservations match accepted orders (%d)", reserved) && ok
	return ok
}

const reservedQty = `
	SELECT coalesce(sum(ri.quantity), 0)
	FROM reservation_items ri JOIN reservations r USING (order_id)
	WHERE ri.product_id = $1 AND r.status IN ('reserved', 'confirmed')`

// --- scenario: payment faults -----------------------------------------------

// faults pays o.orders orders while payment-service injects slow responses,
// then waits for every order to settle and reconciles orders against payments.
func faults(ctx context.Context, o options) bool {
	products := mustPool(ctx, o.productsDB)
	defer products.Close()
	ordersDB := mustPool(ctx, o.ordersDB)
	defer ordersDB.Close()
	payments := mustPool(ctx, o.paymentsDB)
	defer payments.Close()
	_, _ = products.Exec(ctx, `UPDATE products SET stock = 100000000`)

	fmt.Printf("faults: %d orders, %d concurrent\n", o.orders, o.concurrency)
	rec := newRecorder()
	var mu sync.Mutex
	var ids []string
	sem := make(chan struct{}, o.concurrency)
	var wg sync.WaitGroup
	for i := 0; i < o.orders; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			code, created, _, err := createOrder(o.base, "prod-pen-006", 1)
			if err != nil || code != http.StatusCreated {
				rec.add("create", code, err, 0)
				return
			}
			code, _, d, err := payOrder(o.base, created.ID)
			rec.add("pay", code, err, d)
			mu.Lock()
			ids = append(ids, created.ID)
			mu.Unlock()
		}()
	}
	wg.Wait()
	rec.report()
	accepted := rec.codes["pay 202"]
	timedOut := rec.codes["pay 504"]
	fmt.Printf("  %d of %d payments had an unknown outcome (202 payment_pending), %d timed out at the gateway (504)\n", accepted, len(ids), timedOut)

	// Wait for the recovery worker to settle every payment_pending order.
	start := time.Now()
	var open int
	for {
		_ = ordersDB.QueryRow(ctx, `SELECT count(*) FROM orders WHERE id = ANY($1) AND status = 'payment_pending'`, ids).Scan(&open)
		if open == 0 || time.Since(start) > 120*time.Second {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("  recovery settled all orders in %.1fs (%d still pending)\n", time.Since(start).Seconds(), open)

	var paidOrders int
	_ = ordersDB.QueryRow(ctx, `SELECT count(*) FROM orders WHERE id = ANY($1) AND status = 'paid'`, ids).Scan(&paidOrders)
	var succeeded, duplicate int
	_ = payments.QueryRow(ctx, `SELECT count(*) FROM payments WHERE order_id = ANY($1) AND status = 'succeeded'`, ids).Scan(&succeeded)
	_ = payments.QueryRow(ctx, `
		SELECT count(*) FROM (SELECT order_id FROM payments WHERE order_id = ANY($1)
		GROUP BY order_id HAVING count(*) > 1) d`, ids).Scan(&duplicate)

	ok := true
	ok = check(accepted > 0, "faults were actually injected (%d unknown outcomes)", accepted) && ok
	ok = check(timedOut == 0, "unknown outcomes reported as 202, not gateway timeouts") && ok
	ok = check(open == 0, "no order left in payment_pending") && ok
	ok = check(paidOrders == len(ids), "all %d orders paid (%d)", len(ids), paidOrders) && ok
	ok = check(succeeded == paidOrders, "payments %d = paid orders %d (no charge without a paid order)", succeeded, paidOrders) && ok
	ok = check(duplicate == 0, "zero double charges") && ok
	return ok
}
