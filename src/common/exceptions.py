"""Project-specific exceptions that make pipeline failures easier to understand."""


class PipelineError(Exception):
    """Base exception for expected pipeline failures."""


class ExtractionError(PipelineError):
    """Raised when source data cannot be downloaded."""


class ValidationError(PipelineError):
    """Raised when downloaded data does not match the expected contract."""


class StorageError(PipelineError):
    """Raised when data cannot be stored in the data lake."""


class MetadataError(PipelineError):
    """Raised when a pipeline run cannot be recorded in PostgreSQL."""
