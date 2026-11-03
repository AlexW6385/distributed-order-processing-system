// Command loadtest drives the public HTTP API and checks the system's
// invariants under load.
//
//	go run ./loadtest smoke                           # API behaviour, end to end
//	go run ./loadtest verify                          # cross-service invariants over all data
//	go run ./loadtest checkout -c 200 -d 60s
//	go run ./loadtest oversell -orders 1000 -units 100
//	go run ./loadtest faults   -orders 500            # payment-service needs PAYMENT_FAULT_RATE > 0
//	go run ./loadtest flashsale -orders 10000 -units 100 [-gate=false]
//	go run ./loadtest chaos    -c 30 -d 60s           # Toxiproxy faults during load, then verify
//	go run ./loadtest catalog  -c 50 -d 20s           # product list throughput and database load
//	go run ./loadtest indexes  -rows 2000000          # query plans with and without the partial indexes
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/migrate"
	ordermigrations "github.com/AlexW6385/distributed-order-processing-system/services/order-service/migrations"
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
	adminDB     string
	notifyDB    string
	redisAddr   string
	toxiproxy   string
	gate        bool
	rows        int
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: loadtest smoke|checkout|oversell|flashsale|faults|chaos|verify|catalog|indexes [flags]")
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
	fs.StringVar(&o.adminDB, "admin-db", pg+"postgres?sslmode=disable", "database used to create a scratch database (indexes)")
	fs.IntVar(&o.rows, "rows", 2_000_000, "rows to seed (indexes)")
	fs.StringVar(&o.notifyDB, "notifications-db", pg+"notifications?sslmode=disable", "notification-service database")
	fs.StringVar(&o.redisAddr, "redis", "localhost:6379", "Redis (flashsale without the gate)")
	fs.StringVar(&o.toxiproxy, "toxiproxy", "http://localhost:8474", "Toxiproxy API (chaos)")
	fs.BoolVar(&o.gate, "gate", true, "use the Redis flash-sale gate (flashsale)")
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
	case "catalog":
		ok = catalog(ctx, o)
	case "smoke":
		ok = smoke(ctx, o)
	case "flashsale":
		ok = flashsale(ctx, o)
	case "chaos":
		ok = chaos(ctx, o)
	case "verify":
		ok = verify(ctx, o)
	case "indexes":
		ok = indexes(ctx, o)
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
	return createOrderFor(base, "load@example.com", productID, qty)
}

func createOrderFor(base, email, productID string, qty int) (int, order, time.Duration, error) {
	return call(http.MethodPost, base+"/orders", "", map[string]any{
		"customer_email": email,
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
	// Skip products on flash sale: the Redis gate limits them to one unit per
	// customer, which is not what this scenario measures.
	rows, _ := products.Query(ctx, `SELECT id FROM products WHERE id NOT IN ('prod-headphones-014', 'prod-speaker-015') ORDER BY id`)
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
	var windowStart, windowEnd time.Time
	_ = orders.QueryRow(ctx, `SELECT now()`).Scan(&windowStart)
	measuring.Store(true)
	began := time.Now()
	time.Sleep(o.duration)
	measuring.Store(false)
	elapsed := time.Since(began)
	_ = orders.QueryRow(ctx, `SELECT now()`).Scan(&windowEnd)
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

	// Publish lag: time from the state change committing to the event being
	// accepted by Kafka, for events created during the measured window.
	var p50, p95, p99, maxLag float64
	_ = orders.QueryRow(ctx, `
		SELECT coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY lag), 0),
		       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY lag), 0),
		       coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY lag), 0),
		       coalesce(max(lag), 0)
		FROM (SELECT extract(epoch FROM published_at - created_at) * 1000 AS lag
		      FROM outbox_events
		      WHERE created_at BETWEEN $1 AND $2 AND published_at IS NOT NULL) l`,
		windowStart, windowEnd).Scan(&p50, &p95, &p99, &maxLag)
	fmt.Printf("  event publish lag: p50 %.0fms, p95 %.0fms, p99 %.0fms, max %.0fms\n", p50, p95, p99, maxLag)

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
			// Like a real client, retry 503s: the circuit breaker refused the
			// call without sending it, so nothing was charged.
			code, _, d, err := payOrder(o.base, created.ID)
			for attempt := 0; code == http.StatusServiceUnavailable && attempt < 300; attempt++ {
				rec.add("pay", code, err, d)
				time.Sleep(200 * time.Millisecond)
				code, _, d, err = payOrder(o.base, created.ID)
			}
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
	return verify(ctx, o) && ok
}

// --- scenario: catalog ------------------------------------------------------

// catalog hammers GET /products and reports how many table scans reached
// Postgres. Run it with and without the cache (PRODUCT_CACHE_TTL=0).
func catalog(ctx context.Context, o options) bool {
	products := mustPool(ctx, o.productsDB)
	defer products.Close()
	scans := func() int64 {
		var n int64
		_ = products.QueryRow(ctx, `SELECT pg_stat_force_next_flush()`).Scan(new(any))
		_ = products.QueryRow(ctx, `SELECT coalesce(seq_scan, 0) FROM pg_stat_user_tables WHERE relname = 'products'`).Scan(&n)
		return n
	}
	time.Sleep(2 * time.Second) // let earlier activity reach the statistics
	before := scans()

	fmt.Printf("catalog: %d virtual users for %s\n", o.concurrency, o.duration)
	rec := newRecorder()
	var requests atomic.Int64
	deadline := time.Now().Add(o.duration)
	var wg sync.WaitGroup
	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				code, _, d, err := call(http.MethodGet, o.base+"/products", "", nil)
				rec.add("list", code, err, d)
				requests.Add(1)
			}
		}()
	}
	wg.Wait()
	time.Sleep(11 * time.Second) // backends flush statistics within ~10s of going idle
	after := scans()

	fmt.Printf("  %d requests = %.0f req/s\n", requests.Load(), float64(requests.Load())/o.duration.Seconds())
	rec.report()
	fmt.Printf("  products table scans in Postgres: %d (%.2f per request)\n", after-before, float64(after-before)/float64(requests.Load()))
	return check(rec.codes["list 200"] == int(requests.Load()), "all requests succeeded")
}

// --- scenario: indexes ------------------------------------------------------

// indexes seeds a large order-service database and times the recovery and
// outbox queries with and without their partial indexes.
func indexes(ctx context.Context, o options) bool {
	admin := mustPool(ctx, o.adminDB)
	defer admin.Close()
	name := fmt.Sprintf("bench_indexes_%d", time.Now().Unix())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return false
	}
	defer admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") //nolint:errcheck
	url := strings.Replace(o.adminDB, "/postgres?", "/"+name+"?", 1)
	db := mustPool(ctx, url)
	defer db.Close()
	if err := migrate.Run(ctx, db, ordermigrations.FS); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return false
	}

	fmt.Printf("indexes: seeding %d orders and %d outbox events...\n", o.rows, o.rows)
	seed := []string{
		// Almost every order is finished; a few hundred are stuck in each
		// open state, which is what the recovery worker looks for.
		`INSERT INTO orders (id, customer_email, status, total_cents, updated_at, expires_at)
		 SELECT 'o' || g, 'u@example.com',
		        CASE WHEN g % 10000 = 0 THEN 'payment_pending'
		             WHEN g % 10000 = 1 THEN 'stock_reserved'
		             WHEN g % 10000 = 2 THEN 'pending'
		             ELSE 'paid' END,
		        1000, now() - interval '1 hour', now() - interval '1 minute'
		 FROM generate_series(1, $1) g`,
		`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status, published_at)
		 SELECT 'e' || g, 'order', 'o' || g, 'order.paid', '{}',
		        CASE WHEN g % 20000 = 0 THEN 'pending' ELSE 'published' END,
		        CASE WHEN g % 20000 = 0 THEN NULL ELSE now() END
		 FROM generate_series(1, $1) g`,
	}
	for _, q := range seed {
		if _, err := db.Exec(ctx, q, o.rows); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return false
		}
	}
	_, _ = db.Exec(ctx, `VACUUM ANALYZE`)

	queries := []struct{ name, sql string }{
		{"recovery: stuck payments", `SELECT id FROM orders WHERE status = 'payment_pending' AND updated_at < now() - make_interval(secs => 10) ORDER BY updated_at LIMIT 50`},
		{"recovery: expired reservations", `SELECT id FROM orders WHERE status = 'stock_reserved' AND expires_at <= now() ORDER BY expires_at LIMIT 50`},
		{"outbox: claim due events", `SELECT id FROM outbox_events WHERE status = 'pending' AND next_attempt_at <= now() ORDER BY created_at LIMIT 100`},
	}
	run := func() []float64 {
		var out []float64
		for _, q := range queries {
			var times []float64
			for i := 0; i < 7; i++ {
				var plan []map[string]any
				if err := db.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+q.sql).Scan(&plan); err != nil {
					fmt.Fprintln(os.Stderr, err)
					return nil
				}
				times = append(times, plan[0]["Execution Time"].(float64))
			}
			sort.Float64s(times)
			out = append(out, times[len(times)/2])
		}
		return out
	}
	size := func(index string) string {
		var s string
		_ = db.QueryRow(ctx, `SELECT pg_size_pretty(pg_relation_size($1))`, index).Scan(&s)
		return s
	}

	with := run()
	sizes := []string{size("idx_orders_open_status"), size("idx_outbox_events_pending")}
	_, _ = db.Exec(ctx, `DROP INDEX idx_orders_open_status; DROP INDEX idx_outbox_events_pending`)
	_, _ = db.Exec(ctx, `ANALYZE`)
	without := run()
	// What a conventional full index would look like, for size comparison.
	_, _ = db.Exec(ctx, `CREATE INDEX full_orders_status ON orders (status, updated_at); CREATE INDEX full_outbox_status ON outbox_events (status, created_at)`)
	fullSizes := []string{size("full_orders_status"), size("full_outbox_status")}

	fmt.Printf("  %-32s %14s %14s %10s\n", "query (median of 7)", "no index", "partial index", "speedup")
	for i, q := range queries {
		fmt.Printf("  %-32s %12.2fms %12.3fms %9.0fx\n", q.name, without[i], with[i], without[i]/with[i])
	}
	fmt.Printf("  index size: orders partial %s vs full %s; outbox partial %s vs full %s\n", sizes[0], fullSizes[0], sizes[1], fullSizes[1])
	return true
}
