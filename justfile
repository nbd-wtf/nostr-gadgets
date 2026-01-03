nothing:
    @just --list

publish:
    jsr publish

test:
    bun test --timeout=30000
