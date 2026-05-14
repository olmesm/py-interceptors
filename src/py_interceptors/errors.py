class ValidationError(Exception):
    """Raised when chain structure, metadata, or policy declarations are invalid."""

    pass


class CompilationError(Exception):
    """Raised when runtime compilation fails outside normal validation errors."""

    pass


class ExecutionError(Exception):
    """Raised when a validated workflow cannot be executed as requested."""

    pass


class DependencyError(ValidationError):
    """Base class for dependency-resolution failures detected during validation."""

    pass


class UnknownDependencyError(DependencyError):
    """Raised when ``.use(Cls, name=...)`` binds a name not declared on ``Cls``."""

    pass


class MissingDependencyError(DependencyError):
    """Raised when a declared dependency cannot be resolved from any ancestor."""

    pass


class AmbiguousDependencyError(DependencyError):
    """Raised when type-only resolution finds multiple candidates at the same chain."""

    pass


class DependencyTypeError(DependencyError):
    """Raised when a bound or resolved value is incompatible with the declared type."""

    pass
