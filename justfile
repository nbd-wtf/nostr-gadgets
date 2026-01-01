nothing:
    @just --list

publish:
    jsr publish

test:
    bun test --timeout=30000

build-redstore:
    cd redstore && ../node_modules/.bin/wasm-pack build --target web --out-dir pkg --dev
