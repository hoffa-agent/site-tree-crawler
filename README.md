# site-tree-crawler

A small Go CLI that crawls a website from a single domain/URL and renders a proper live Bubble Tea terminal UI of directories/files discovered from HTML links and asset references.

```bash
go run . example.com
# or
go build -o site-tree && ./site-tree https://example.com --max-pages 500
```

Options:

- `--max-pages` max HTML pages to crawl, default `250`
- `--workers` concurrent fetch workers, default `8`
- `--plain` print final tree only, useful for logs/CI or terminals without TUI support
- `--insecure` skip TLS verification for broken sites

It stays on the same host (treating `www.` and bare domain as equivalent), strips query strings, and infers files by extensions.
