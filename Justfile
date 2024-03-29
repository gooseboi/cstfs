build:
	cargo -Z build-std build --target=x86_64-unknown-linux-gnu

build_release:
	cargo -Z build-std build --target=x86_64-unknown-linux-gnu --release

run *ARGS: build
	./target/x86_64-unknown-linux-gnu/debug/cstfs {{ARGS}}

run_release *ARGS: build_release
	./target/x86_64-unknown-linux-gnu/release/cstfs {{ARGS}}

check:
	cargo clippy --all-targets --all-features
