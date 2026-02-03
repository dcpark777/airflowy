"""
Smart retry layer for Databricks/Spark job failures.

This module provides configurable retry behavior that can be enabled/disabled
and used as a layer on top of operators. It classifies errors into:
- No retry: code/config/auth errors
- Retry immediately: transient API/network errors
- Retry after backoff: throttling, resource availability
"""

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

# Retry behavior constants
RETRY_BEHAVIOR_NO_RETRY = "no_retry"
RETRY_BEHAVIOR_IMMEDIATE = "retry_immediate"
RETRY_BEHAVIOR_BACKOFF = "retry_backoff"

DEFAULT_RETRY_BACKOFF_SECONDS = 60
DEFAULT_MAX_TOTAL_RETRIES = 5


@dataclass(frozen=True)
class RetryConfig:
    """
    Configuration for the smart retry layer.

    Pass to operators or use with run_with_retries() to enable/disable
    and tune retry behavior without changing operator code.

    Attributes:
        enabled: If False, no retries are attempted; failures raise immediately.
        default_retry_backoff_seconds: Delay (seconds) when classification
            is RETRY_BEHAVIOR_BACKOFF (e.g. throttling). Default 60.
        max_total_retries: Cap on total retry attempts to avoid unbounded loops.
            Default 5.
    """

    enabled: bool = True
    default_retry_backoff_seconds: int = DEFAULT_RETRY_BACKOFF_SECONDS
    max_total_retries: int = DEFAULT_MAX_TOTAL_RETRIES


def default_retryable_patterns() -> Dict[str, Tuple[str, int, Optional[int]]]:
    """
    Default retryable error patterns: (reason, max_retries, retry_delay_seconds).
    delay 0 = immediate; None = use config default backoff.
    """
    immediate = 0
    backoff = None
    return {
        "connection": ("Connection error - network issue", 3, immediate),
        "timeout": ("Timeout error - may resolve on retry", 3, immediate),
        "connection reset": ("Connection reset - transient network issue", 3, immediate),
        "connection refused": (
            "Connection refused - service may be temporarily unavailable",
            3,
            immediate,
        ),
        "network": ("Network error - transient issue", 3, immediate),
        "service unavailable": ("Service unavailable - transient", 3, immediate),
        "503": ("Service unavailable (503)", 3, immediate),
        "502": ("Bad gateway (502) - transient", 3, immediate),
        "504": ("Gateway timeout (504)", 3, immediate),
        "read timeout": ("Read timeout - network issue", 3, immediate),
        "write timeout": ("Write timeout - network issue", 3, immediate),
        "temporary failure": ("Temporary failure - will retry", 3, immediate),
        "temporary error": ("Temporary error - will retry", 3, immediate),
        "retry": ("Error suggests retry", 3, immediate),
        "throttl": ("Rate limiting - retry after backoff", 3, backoff),
        "rate limit": ("Rate limit exceeded - retry after backoff", 3, backoff),
        "out of memory": ("Out of memory - retry after backoff", 2, backoff),
        "no space left": ("Disk space issue - retry after backoff", 2, backoff),
        "resource": ("Resource unavailable - retry after backoff", 3, backoff),
        "cluster": ("Cluster issue - retry after backoff", 3, backoff),
        "cluster terminated": ("Cluster terminated unexpectedly", 2, backoff),
        "spot instance": ("Spot instance interruption", 3, backoff),
        "instance terminated": ("Instance terminated - transient", 3, backoff),
        "execution timeout": ("Execution timeout - retry after backoff", 2, backoff),
        "lock": ("Lock contention - retry after backoff", 2, backoff),
        "deadlock": ("Deadlock detected - retry after backoff", 2, backoff),
    }


def default_non_retryable_patterns() -> Dict[str, str]:
    """Default non-retryable error patterns: pattern -> reason."""
    return {
        "syntax error": "Syntax error in code",
        "parse error": "Parse error - code issue",
        "compilation error": "Compilation error - code issue",
        "nameerror": "NameError - undefined variable or function",
        "typeerror": "TypeError - type mismatch",
        "attributeerror": "AttributeError - missing attribute",
        "indentationerror": "IndentationError - code formatting issue",
        "configuration error": "Configuration error - invalid settings",
        "invalid configuration": "Invalid configuration",
        "missing configuration": "Missing required configuration",
        "config": "Configuration issue",
        "authentication": "Authentication failed - credentials issue",
        "authorization": "Authorization failed - permission issue",
        "unauthorized": "Unauthorized access",
        "forbidden": "Forbidden - access denied",
        "401": "Unauthorized (401)",
        "403": "Forbidden (403)",
        "data validation": "Data validation failed",
        "invalid data": "Invalid data format or content",
        "schema": "Schema mismatch or validation error",
        "null pointer": "Null pointer exception - data issue",
        "nullreference": "Null reference - data issue",
        "file not found": "File not found - path issue",
        "no such file": "File does not exist",
        "directory not found": "Directory does not exist",
        "path not found": "Path does not exist",
        "assertion": "Assertion failed - logic error",
        "illegal argument": "Illegal argument - invalid input",
        "illegal state": "Illegal state - logic error",
        "index out of bounds": "Index out of bounds - logic error",
        "version": "Version mismatch or incompatibility",
        "incompatible": "Incompatible version or format",
        "not supported": "Feature not supported",
        "400": "Bad request (400) - invalid request",
        "404": "Not found (404) - resource does not exist",
        "405": "Method not allowed (405)",
        "409": "Conflict (409) - resource conflict",
        "422": "Unprocessable entity (422) - validation error",
    }


def classify_error(
    error_message: str,
    default_backoff_seconds: int,
    retryable_patterns: Optional[Dict[str, Tuple[str, int, Optional[int]]]] = None,
    non_retryable_patterns: Optional[Dict[str, str]] = None,
) -> Tuple[str, str, int, int]:
    """
    Classify an error message into retry behavior.

    Returns:
        (retry_behavior, reason, max_retries, retry_delay_seconds)
    """
    retryable = retryable_patterns if retryable_patterns is not None else default_retryable_patterns()
    non_retryable = (
        non_retryable_patterns
        if non_retryable_patterns is not None
        else default_non_retryable_patterns()
    )

    if not error_message:
        return (
            RETRY_BEHAVIOR_IMMEDIATE,
            "Empty error message - allowing retry",
            3,
            0,
        )

    error_lower = error_message.lower()

    for pattern, reason in sorted(non_retryable.items(), key=lambda x: len(x[0]), reverse=True):
        if pattern.lower() in error_lower:
            return (RETRY_BEHAVIOR_NO_RETRY, f"Non-retryable error: {reason}", 0, 0)

    backoff = default_backoff_seconds
    for pattern, (reason, max_retries, delay_spec) in sorted(
        retryable.items(), key=lambda x: len(x[0]), reverse=True
    ):
        if pattern.lower() in error_lower:
            delay = delay_spec if delay_spec is not None else backoff
            behavior = RETRY_BEHAVIOR_BACKOFF if delay > 0 else RETRY_BEHAVIOR_IMMEDIATE
            return (behavior, f"Retryable error: {reason}", max_retries, delay)

    return (
        RETRY_BEHAVIOR_BACKOFF,
        "Unknown error - allowing retry with backoff",
        2,
        backoff,
    )


def run_with_retries(
    run_fn: Callable[[Any], Any],
    context: Any,
    config: RetryConfig,
    log: Any,
    *,
    get_error_message: Optional[Callable[[Exception], str]] = None,
    retryable_patterns: Optional[Dict[str, Tuple[str, int, Optional[int]]]] = None,
    non_retryable_patterns: Optional[Dict[str, str]] = None,
    on_before_raise: Optional[Callable[[], None]] = None,
) -> Any:
    """
    Run a callable with the smart retry layer.

    When config.enabled is False, runs run_fn(context) once and raises on failure.
    Otherwise classifies failures and retries (immediate or after backoff) up to
    config.max_total_retries.

    Args:
        run_fn: Callable that takes context and returns result; raises on failure.
        context: Passed to run_fn.
        config: Retry configuration (enabled, backoff, max retries).
        log: Logger-like object with .warning() and .info().
        get_error_message: Optional. Extract message from exception (default: str(e)).
        retryable_patterns: Optional. Override default retryable patterns.
        non_retryable_patterns: Optional. Override default non-retryable patterns.
        on_before_raise: Optional. Called before re-raising (e.g. to log job output).

    Returns:
        Return value of run_fn(context) on success.

    Raises:
        The last exception from run_fn when retries are exhausted or
        failure is classified as non-retryable.
    """
    if not config.enabled:
        return run_fn(context)

    def _get_msg(e: Exception) -> str:
        if get_error_message:
            return get_error_message(e)
        return str(e) or getattr(e, "message", "") or ""

    attempt = 0
    while True:
        attempt += 1
        try:
            return run_fn(context)
        except Exception as e:
            if attempt > config.max_total_retries:
                log.warning("Max total retries (%s) exceeded.", config.max_total_retries)
                if on_before_raise:
                    on_before_raise()
                raise

            msg = _get_msg(e)
            behavior, reason, max_retries, retry_delay_seconds = classify_error(
                msg,
                config.default_retry_backoff_seconds,
                retryable_patterns=retryable_patterns,
                non_retryable_patterns=non_retryable_patterns,
            )

            if behavior == RETRY_BEHAVIOR_NO_RETRY:
                log.warning("Failure classified as non-retryable: %s", reason)
                if on_before_raise:
                    on_before_raise()
                raise

            if max_retries <= 0:
                if on_before_raise:
                    on_before_raise()
                raise

            log.warning(
                "Attempt %s failed (%s). Retrying (delay=%ss). Reason: %s",
                attempt,
                reason,
                retry_delay_seconds,
                msg[:200] + ("..." if len(msg) > 200 else ""),
            )
            if retry_delay_seconds > 0:
                log.info(
                    "Sleeping %s seconds before retry (backoff).",
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
