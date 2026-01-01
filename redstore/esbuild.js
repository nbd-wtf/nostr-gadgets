import * as esbuild from 'esbuild'

const isWatchMode = process.argv.includes('--watch')

const config = {
  entryPoints: ['example/app.ts', 'worker.ts'],
  bundle: true,
  outdir: 'example/dist',
  format: 'esm',
  platform: 'browser',
  sourcemap: true,
  external: ['*.wasm'],
  logLevel: 'info',
}

if (isWatchMode) {
  const ctx = await esbuild.context(config)
  await ctx.watch()
  console.log('watching for changes...')
} else {
  await esbuild.build(config)
  console.log('build complete')
}
