nothing:
    @just --list

publish:
    jsr publish

test:
    deno test --allow-net --no-check --unstable-sloppy-imports metadata.test.ts
