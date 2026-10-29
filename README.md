# OpVault Server Rust
This is a port of the [Operation Vault](https://github.com/Infinity6542/Operation-Vault) server in Rust (from Go). It includes:
- Multiple [WebTransport](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport) (WT) endpoints that act similarly to HTTP POST/GET endpoints
- Backup storage and fetching from any S3-compatible storage (currently my Cloudflare R2 bucket is hardcoded)
- Basically everything that the Go server can do

It's not really stable and there are some weird things that have to be ironed out, probably because Go and Rust aren't that similar. They should functionally be similar, but there are small things that need to be ironed out.
## Usage
Please follow the installation guide from the main repo (https://github.com/Infinity6542/Operation-Vault-Plugin#installation--usage).

1. Once installed, head to Settings (gear icon on the bottom of the screen).
2. Scroll on the left sidebar until you see "Operation Vault". Click on it.
3. For the "Server URL" field, replace whatever's currently prefilled with `https://rust.opal.jchen.au:7200/ws`.
4. Reload the app. This can be done by closing and reopening Obsidian, or opening the command palette (Ctrl+P) then searching and selecting "Reload app without saving".
