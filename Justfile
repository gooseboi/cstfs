build:
	cargo -Z build-std build --target=x86_64-unknown-linux-gnu

build_release:
	cargo -Z build-std build --target=x86_64-unknown-linux-gnu --release

run: build
	./target/x86_64-unknown-linux-gnu/debug/cstfs

run_release: build_release
	./target/x86_64-unknown-linux-gnu/release/cstfs

check:
	cargo clippy --all-targets --all-features
