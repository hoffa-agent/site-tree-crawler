package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type node struct {
	Name     string
	IsFile   bool
	Children map[string]*node
}

type tree struct {
	mu   sync.RWMutex
	root *node
	seen map[string]bool
}

func newTree() *tree {
	return &tree{root: &node{Name: "/", Children: map[string]*node{}}, seen: map[string]bool{}}
}

func (t *tree) add(rawPath string) bool {
	if rawPath == "" {
		rawPath = "/"
	}
	rawPath = cleanPath(rawPath)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.seen[rawPath] {
		return false
	}
	t.seen[rawPath] = true
	if rawPath == "/" {
		return true
	}
	parts := strings.Split(strings.Trim(rawPath, "/"), "/")
	cur := t.root
	for i, p := range parts {
		if p == "" {
			continue
		}
		if cur.Children == nil {
			cur.Children = map[string]*node{}
		}
		child, ok := cur.Children[p]
		if !ok {
			child = &node{Name: p, Children: map[string]*node{}}
			cur.Children[p] = child
		}
		if i == len(parts)-1 && looksLikeFile(p) {
			child.IsFile = true
		}
		cur = child
	}
	return true
}

func (t *tree) count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.seen)
}

func (t *tree) render(rootLabel string, scanned, queued, found int64, current string, done bool) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var b strings.Builder
	status := "scanning"
	if done {
		status = "done"
	}
	fmt.Fprintf(&b, "site-tree %s  | scanned: %d  queued: %d  found: %d\n", status, scanned, queued, found)
	if current != "" {
		fmt.Fprintf(&b, "current: %s\n", current)
	}
	fmt.Fprintf(&b, "\n%s\n", rootLabel)
	children := sortedChildren(t.root)
	for i, c := range children {
		last := i == len(children)-1
		renderNode(&b, c, "", last)
	}
	return b.String()
}

func sortedChildren(n *node) []*node {
	kids := make([]*node, 0, len(n.Children))
	for _, c := range n.Children {
		kids = append(kids, c)
	}
	sort.Slice(kids, func(i, j int) bool {
		if kids[i].IsFile != kids[j].IsFile {
			return !kids[i].IsFile
		}
		return strings.ToLower(kids[i].Name) < strings.ToLower(kids[j].Name)
	})
	return kids
}

func renderNode(b *strings.Builder, n *node, prefix string, last bool) {
	branch := "├── "
	nextPrefix := prefix + "│   "
	if last {
		branch = "└── "
		nextPrefix = prefix + "    "
	}
	icon := "📁 "
	if n.IsFile {
		icon = "📄 "
	}
	fmt.Fprintf(b, "%s%s%s%s\n", prefix, branch, icon, n.Name)
	kids := sortedChildren(n)
	for i, c := range kids {
		renderNode(b, c, nextPrefix, i == len(kids)-1)
	}
}

type crawler struct {
	base      *url.URL
	client    *http.Client
	tree      *tree
	maxPages  int
	workers   int
	pagesSeen sync.Map
	queue     chan string
	scanned   int64
	queued    int64
	found     int64
	active    int64
	current   atomic.Value
}

func newCrawler(base *url.URL, maxPages, workers int, insecure bool) *crawler {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure}} //nolint:gosec user-requested scanner option
	return &crawler{base: base, client: &http.Client{Timeout: 12 * time.Second, Transport: tr}, tree: newTree(), maxPages: maxPages, workers: workers, queue: make(chan string, maxPages*2)}
}

func (c *crawler) enqueue(u string) {
	if atomic.LoadInt64(&c.queued) >= int64(c.maxPages) {
		return
	}
	if _, loaded := c.pagesSeen.LoadOrStore(u, true); loaded {
		return
	}
	atomic.AddInt64(&c.queued, 1)
	select {
	case c.queue <- u:
	default:
	}
}

func (c *crawler) run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.worker(ctx) }()
	}
	c.enqueue(c.base.String())
	for {
		if ctx.Err() != nil {
			break
		}
		if atomic.LoadInt64(&c.active) == 0 && len(c.queue) == 0 {
			break
		}
		if atomic.LoadInt64(&c.scanned) >= int64(c.maxPages) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(c.queue)
	wg.Wait()
}

func (c *crawler) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case u, ok := <-c.queue:
			if !ok {
				return
			}
			atomic.AddInt64(&c.active, 1)
			c.scan(ctx, u)
			atomic.AddInt64(&c.active, -1)
		}
	}
}

func (c *crawler) scan(ctx context.Context, raw string) {
	if atomic.LoadInt64(&c.scanned) >= int64(c.maxPages) {
		return
	}
	c.current.Store(raw)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "site-tree-crawler/0.1 (+https://github.com)")
	resp, err := c.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	atomic.AddInt64(&c.scanned, 1)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if c.tree.add(resp.Request.URL.Path) {
		atomic.AddInt64(&c.found, 1)
	}
	if !strings.Contains(ct, "text/html") {
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 3<<20))
	if err != nil {
		return
	}
	for _, ref := range extractRefs(string(body)) {
		u, ok := c.resolve(ref)
		if !ok {
			continue
		}
		if c.tree.add(u.Path) {
			atomic.AddInt64(&c.found, 1)
		}
		if shouldCrawl(u.Path) {
			c.enqueue(u.String())
		}
	}
}

func (c *crawler) resolve(ref string) (*url.URL, bool) {
	ref = strings.TrimSpace(htmlUnescape(ref))
	if ref == "" || strings.HasPrefix(ref, "#") || strings.HasPrefix(ref, "mailto:") || strings.HasPrefix(ref, "tel:") || strings.HasPrefix(ref, "javascript:") || strings.HasPrefix(ref, "data:") {
		return nil, false
	}
	u, err := url.Parse(ref)
	if err != nil {
		return nil, false
	}
	u = c.base.ResolveReference(u)
	u.Fragment = ""
	u.RawQuery = ""
	if !sameHost(c.base, u) {
		return nil, false
	}
	return u, true
}

var refRE = regexp.MustCompile(`(?i)(?:href|src|action|poster)\s*=\s*["']([^"'#]+)["']|url\(["']?([^"')]+)["']?\)`)

func extractRefs(html string) []string {
	m := refRE.FindAllStringSubmatch(html, -1)
	out := make([]string, 0, len(m))
	for _, x := range m {
		if x[1] != "" {
			out = append(out, x[1])
		} else if x[2] != "" {
			out = append(out, x[2])
		}
	}
	return out
}

func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	p = "/" + strings.TrimPrefix(p, "/")
	if strings.HasSuffix(p, "/") && p != "/" {
		p = strings.TrimSuffix(p, "/")
	}
	return path.Clean(p)
}

func looksLikeFile(name string) bool {
	if strings.Contains(name, ".") {
		return true
	}
	return false
}

func shouldCrawl(p string) bool {
	ext := strings.ToLower(path.Ext(p))
	if ext == "" {
		return true
	}
	switch ext {
	case ".html", ".htm", ".php", ".asp", ".aspx", ".jsp":
		return true
	}
	return false
}

func sameHost(a, b *url.URL) bool {
	return strings.EqualFold(stripWWW(a.Hostname()), stripWWW(b.Hostname()))
}
func stripWWW(h string) string { return strings.TrimPrefix(strings.ToLower(h), "www.") }

func htmlUnescape(s string) string {
	repl := strings.NewReplacer("&amp;", "&", "&#38;", "&", "&quot;", "\"", "&#34;", "\"", "&#39;", "'", "&apos;", "'")
	return repl.Replace(s)
}

func normalizeInput(s string) (*url.URL, error) {
	if !strings.Contains(s, "://") {
		s = "https://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	if u.Host == "" {
		return nil, fmt.Errorf("missing domain")
	}
	u.Path = "/"
	u.RawQuery = ""
	u.Fragment = ""
	return u, nil
}

func main() {
	maxPages := flag.Int("max-pages", 250, "maximum HTML pages to crawl")
	workers := flag.Int("workers", 8, "concurrent fetch workers")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	interval := flag.Duration("refresh", 180*time.Millisecond, "TUI refresh interval")
	plain := flag.Bool("plain", false, "disable live screen clearing; print final tree only")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options] <domain-or-url>\n\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	base, err := normalizeInput(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	c := newCrawler(base, *maxPages, *workers, *insecure)
	done := make(chan struct{})
	go func() { c.run(ctx); close(done) }()

	if *plain {
		<-done
		fmt.Print(c.tree.render(base.Hostname(), c.scanned, c.queued, c.found, "", true))
		return
	}

	w := bufio.NewWriter(os.Stdout)
	t := time.NewTicker(*interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			fmt.Fprint(w, "\033[H\033[2J")
			cur, _ := c.current.Load().(string)
			fmt.Fprint(w, c.tree.render(base.Hostname(), c.scanned, c.queued, c.found, cur, true))
			w.Flush()
			return
		case <-t.C:
			fmt.Fprint(w, "\033[H\033[2J")
			cur, _ := c.current.Load().(string)
			fmt.Fprint(w, c.tree.render(base.Hostname(), c.scanned, c.queued, c.found, cur, false))
			w.Flush()
		}
	}
}
