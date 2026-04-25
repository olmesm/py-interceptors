class ValidationError(Exception):
    """Raised when chain structure, metadata, or policy declarations are invalid."""

    pass


class CompilationError(Exception):
    """Raised when runtime compilation fails outside normal validation errors."""

    pass


class ExecutionError(Exception):
    """Raised when a validated workflow cannot be executed as requested."""

    pass
