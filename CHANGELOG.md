# Changelog

<!-- version list -->

## v0.1.1 (2026-10-06)

### Bug Fixes

- resolve dependencies per execution scope and harden the runtime ([`492cbb3`](https://github.com/olmesm/py-interceptors/commit/492cbb3c9b51fff6ea8371624ccdd550fdf1d216))

## v0.1.0 (2026-05-14)

### Features

- Add interceptor dependencies via `.use(Cls, **kwargs)` and `.provide()` ([`4761969`](https://github.com/olmesm/py-interceptors/commit/47619697db842a8c37d694b83da16cb0b099e7f2))
- Add `Runtime.run_blocking` for sync callers driving async chains ([`31702b1`](https://github.com/olmesm/py-interceptors/commit/31702b157a50efdaffcec92151fb20bd071c7902))
- Initial release: typed interceptor chains, stream stages, and a Runtime with thread, pool and async policies ([`40ccb6e`](https://github.com/olmesm/py-interceptors/commit/40ccb6e59d4a375d8a99b0d7fdbc6a738c7892d1))
