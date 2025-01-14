nothing:
    @just --list

publish:
    jsr publish

test:
    deno test --allow-net --allow-import --unstable-sloppy-imports metadata.test.ts
