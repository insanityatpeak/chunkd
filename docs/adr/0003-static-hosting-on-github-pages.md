# 0003. Static hosting on GitHub Pages

Status: accepted
Date: 2026-09-29

## Context

The live demo is the sim cluster compiled to WASM plus a Preact dashboard. It needs no server: every byte is a static file. Total size is about 8 MB, dominated by `cluster.wasm`. The audience is people clicking a link from a README or CV.

## Options considered

| | GitHub Pages | Cloudflare Pages | Cloudflare Workers static assets |
|---|---|---|---|
| Setup | Built into the repo; deploys from Actions with the built-in token | Cloudflare account + API token secret | Same as Pages; Cloudflare now points new projects here |
| URL | `insanityatpeak.github.io/chunkd`, same identity as the repo | `*.pages.dev` or custom domain | `*.workers.dev` or custom domain |
| Per-file limit | 100 MiB | 25 MiB | 25 MiB |
| Site limit | 1 GB | 20,000 files | 20,000 files (free) |
| Bandwidth | Soft 100 GB/month | Unlimited static | Unlimited static |
| Custom headers (COOP/COEP, CSP) | No | `_headers` | `_headers` |
| Preview deploy per PR | No | Yes | Yes |
| Server-side code | None | Functions | Workers |

## Decision

GitHub Pages, deployed by `.github/workflows/pages.yml` with `actions/upload-pages-artifact` and `actions/deploy-pages`. Vite builds with `base: '/chunkd/'`. Pages serves `.wasm` as `application/wasm`, so `WebAssembly.instantiateStreaming` works without header configuration; the worker falls back to a buffered compile if it does not.

Cloudflare's advantages do not apply yet:

- Custom headers matter mainly for COOP/COEP, which enable `SharedArrayBuffer` and threads. Go's WASM target is single-threaded, so there is nothing to enable.
- At 8 MB per visit, 100 GB/month covers about 12,500 full loads, far above expected traffic.
- Workers cannot run the real cluster (Go processes with gRPC and disks), and the demo is static by design.

## Consequences

- No secrets, no second account. The demo URL shares the repo's identity.
- No per-PR previews; changes are checked locally with `npm run preview` before merging.
- The 25 MiB per-file limit on Cloudflare would sit just above our 20 MiB WASM budget; GitHub's 100 MiB limit leaves room.

## At 100× scale

At 100× traffic (about 1 TB/month) the soft bandwidth limit is exceeded and GitHub may ask us to move. The build output is plain files in `web/dist`, so moving means replacing the two deploy steps with `wrangler deploy` and changing Vite's `base`. No application code changes.
