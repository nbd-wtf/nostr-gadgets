nothing:
    @just --list

publish:
    jsr publish

test:
    ./node_modules/.bin/vitest --exclude redstore

test-only testName:
    ./node_modules/.bin/vitest -t {{testName}} --exclude redstore
