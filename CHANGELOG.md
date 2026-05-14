# CHANGELOG


## v0.1.0 (2026-05-14)

### Code Style

- Run ruff formatter to satisfy CI format checks
  ([`9721937`](https://github.com/olmesm/py-interceptors/commit/9721937e70b31015d3d017ba7728b4b82a53cc33))

Agent-Logs-Url:
  https://github.com/olmesm/py-interceptors/sessions/94ffb5f0-2bfe-4366-9f50-c04b50a09c53

Co-authored-by: olmesm <16165237+olmesm@users.noreply.github.com>

### Continuous Integration

- Add CI and semantic-release pipelines
  ([`636cac6`](https://github.com/olmesm/py-interceptors/commit/636cac652c3990b02c7b056180d2d47ad8ed4689))

- CI on pull_request and push to main: ruff, mypy, pytest, uv build - Release on push to main using
  python-semantic-release - Publishes to PyPI via OIDC trusted publishing and creates GitHub
  Releases - Versions sourced from pyproject.toml [project].version - Conventional Commits drive
  version bumps (feat=minor, fix/perf=patch)

### Features

- Add interceptor dependencies via .use(Cls, **kwargs) and .provide()
  ([`4761969`](https://github.com/olmesm/py-interceptors/commit/47619697db842a8c37d694b83da16cb0b099e7f2))

Introduces a small dependency injection system on top of Chain and Interceptor:

- Interceptors declare collaborators as annotated class attributes. - .use(Cls, **kwargs) binds a
  value directly to a single step. Unknown kwargs raise UnknownDependencyError; wrong-typed values
  raise DependencyTypeError. - .provide(**kwargs) on a Chain (or builder) pushes an ambient scope
  every nested step can resolve against. - Resolution at compile time: direct bind wins; otherwise
  walk the provide stack nearest-to-root, preferring a name match, falling back to a single type
  match per scope. Class-level defaults make a dependency optional. Missing required deps raise
  MissingDependencyError; multiple type matches in a scope raise AmbiguousDependencyError. -
  Resolved attributes are injected onto each instance just before its enter()/leave()/error()
  lifecycle runs, scoped to the plan execution via a ContextVar.

Adds docs/dependencies.md and a README pointer.

Covered by 17 new tests in tests/test_dependencies.py. Full suite (227) plus mypy and ruff pass.
