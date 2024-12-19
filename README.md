# Clak
Minimal Rust implementation of [SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) group membership protocol.

## Examples
Create cluster:
```bash
HOST=127.0.0.1 PORT=4242 RUST_LOG=info cargo run --example basic  
```

Join cluster:
```bash
HOST=127.0.0.1 PORT=4243 JOIN_HOST=127.0.0.1 JOIN_PORT=4242 RUST_LOG=info cargo run --example basic
```

Subscribe to events:
```bash
HOST=127.0.0.1 PORT=4244 JOIN_HOST=127.0.0.1 JOIN_PORT=4242 cargo run --example subscription
```