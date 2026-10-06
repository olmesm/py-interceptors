# Changelog

<!-- version list -->

## v0.2.1 (2026-10-06)

### Bug Fixes

- reject a StreamChain passed directly to the runtime with a clear error ([`9e5625a`](https://github.com/olmesm/py-interceptors/commit/9e5625a526cb3391bcc1542a9bf3b93157ec07ae))

### Documentation

- match the docs to the code and document what was missing ([`c007d70`](https://github.com/olmesm/py-interceptors/commit/c007d70a4b63cf84e41029b99f6a69cd212861ce))
- one FastAPI example, no polars, examples imported as a package ([`5cf0628`](https://github.com/olmesm/py-interceptors/commit/5cf06284ffb3d4d098f1fc099bb4dcf0d1a34e35))

## v0.2.0 (2026-10-06)

### Refactoring

- collapse builders, dedupe the runtime, drop unused public names ([`1cdf643`](https://github.com/olmesm/py-interceptors/commit/1cdf643f2c26c42d577a87a474f6eb78dca8499e))

### Breaking Changes

- Portal, ExecutionPolicy, Context, CompilationError, Runtime.startup() and Runtime.validate() are removed. Policies take plain string names. Policy (the union) is exported in their place. Use `async with Runtime()` instead of startup(). BoundInterceptor is no longer exported.

## v0.1.1 (2026-10-06)

### Bug Fixes

- resolve dependencies per execution scope and harden the runtime ([`492cbb3`](https://github.com/olmesm/py-interceptors/commit/492cbb3c9b51fff6ea8371624ccdd550fdf1d216))

## v0.1.0 (2026-05-14)

### Features

- Add interceptor dependencies via `.use(Cls, **kwargs)` and `.provide()` ([`4761969`](https://github.com/olmesm/py-interceptors/commit/47619697db842a8c37d694b83da16cb0b099e7f2))
- Add `Runtime.run_blocking` for sync callers driving async chains ([`31702b1`](https://github.com/olmesm/py-interceptors/commit/31702b157a50efdaffcec92151fb20bd071c7902))
- Initial release: typed interceptor chains, stream stages, and a Runtime with thread, pool and async policies ([`40ccb6e`](https://github.com/olmesm/py-interceptors/commit/40ccb6e59d4a375d8a99b0d7fdbc6a738c7892d1))
