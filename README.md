# site-tree-crawler

A small Go CLI that crawls a website from a single domain/URL and renders a live terminal tree of directories/files discovered from HTML links and asset references.

```bash
go run . example.com
# or
go build -o site-tree && ./site-tree https://example.com --max-pages 500
```

Options:

- `--max-pages` max HTML pages to crawl, default `250`
- `--workers` concurrent fetch workers, default `8`
- `--refresh` live TUI refresh interval, default `180ms`
- `--plain` print final tree only, useful for logs/CI
- `--insecure` skip TLS verification for broken sites

It stays on the same host (treating `www.` and bare domain as equivalent), strips query strings, and infers files by extensions.
