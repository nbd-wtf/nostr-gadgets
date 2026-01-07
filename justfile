nothing:
    @just --list

publish:
    jsr publish

build-redstore:
    cd redstore && ../node_modules/.bin/wasm-pack build --target web --out-dir pkg --dev

build-redstore-prod:
    cd redstore && ../node_modules/.bin/wasm-pack build --target web --out-dir pkg --release

example: build-redstore
    cd redstore/example && ../../node_modules/.bin/vite dev

test: build-redstore
    ./node_modules/.bin/vitest

test-only testName: build-redstore
    ./node_modules/.bin/vitest -t {{testName}}
