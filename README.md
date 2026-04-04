# OpVault Server Rust
This is a port of the [Operation Vault](https://github.com/Infinity6542/Operation-Vault) server in Rust (from Go). It includes:
- Multiple [WebTransport](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport) (WT) endpoints that act similarly to HTTP POST/GET endpoints
- Backup storage and fetching from any S3-compatible storage (currently my Cloudflare R2 bucket is hardcoded)
- Basically everything that the Go server can do

It's not really stable and there are some weird things that have to be ironed out, probably because Go and Rust aren't that similar.
