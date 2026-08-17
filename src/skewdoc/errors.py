"""One exception hierarchy, so the CLI maps failures to exit codes without guessing.

Every message names the thing to act on: the file and line for a parse failure, the config key
for a settings error, the missing package for an optional dependency.
"""

from __future__ import annotations


class SkewdocError(Exception):
    """Base class for every deliberate failure in this package."""


class ConfigError(SkewdocError):
    """The configuration is unusable: unknown key, impossible value, missing file."""


class EventLogError(SkewdocError):
    """An event log is missing, unreadable, or does not contain what was asked for."""


class DiagnosisError(SkewdocError):
    """A stage cannot be diagnosed, as distinct from being diagnosed as healthy."""


class WorkloadError(SkewdocError):
    """A workload or strategy is unknown, or its parameters are impossible."""


class SparkMissingError(SkewdocError):
    """PySpark or a JVM is unavailable, so nothing can be run (diagnosis still works)."""
