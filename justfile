nothing:
    @just --list

publish:
    jsr publish

test:
    bun test --timeout=30000

build-redstore:
    cd redstore && ../node_modules/.bin/wasm-pack build --target web --out-dir pkg --dev

example: build-redstore
    cd redstore && node esbuild.js
    cd redstore && cp pkg/gadgets_redstore_bg.wasm example/dist/
    cd redstore && python3 -m http.server -b localhost 7060 --directory example
