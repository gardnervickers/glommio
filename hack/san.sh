!/bin/bash
RUSTFLAGS="-Z sanitizer=$1" cargo test --target x86_64-unknown-linux-gnu -- --nocapture $2