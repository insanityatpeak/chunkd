# chunkd

[![ci](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml/badge.svg)](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml)
[![pages](https://github.com/insanityatpeak/chunkd/actions/workflows/pages.yml/badge.svg)](https://insanityatpeak.github.io/chunkd/)

A fault-tolerant distributed file store in Go, in the style of GFS and HDFS: a metadata service, storage nodes holding content-addressed chunks, and a gateway.

The same core code runs in two modes:

- `real`: separate processes over gRPC, chunks on disk, wall clock.
- `sim`: one process, in-memory network with fault injection, fake clock, seeded RNG. It compiles to WebAssembly and runs in the browser.

Live demo: https://insanityatpeak.github.io/chunkd/

Status: early bootstrap. See [docs/STATUS.md](docs/STATUS.md).

## License

MIT
