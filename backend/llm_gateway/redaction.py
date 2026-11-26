"""Linear-time zero-ReDoS secret and PII redaction evaluator."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Literal

RedactionMode = Literal["mask", "block", "audit"]

_PEM_HEADER = re.compile("-----" + r"BEGIN ((?:[A-Z0-9]{1,24} ){0,4}PRIVATE KEY)-----")
_EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,120}\.[A-Za-z]{2,16}\b")
_BEARER_PATTERN = re.compile(r"(?i)\bBearer[ \t]+[A-Za-z0-9._~+/=\-]{12,}")
_TOKEN_PREFIX_PATTERN = re.compile(
    r"\b(?:(?:sk|pk|rk|ak)-|(?:gh[pousr]|pat)_|github_pat_)[A-Za-z0-9_\-]{12,}"
)
_GOV_ID_PATTERN = re.compile(r"\b[0-9]{3}-[0-9]{2}-[0-9]{4}\b")

_RULES: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("bearer_authorization", _BEARER_PATTERN, "Bearer [REDACTED:TOKEN]"),
    ("api_token_prefix", _TOKEN_PREFIX_PATTERN, "[REDACTED:API_TOKEN]"),
    ("email_address", _EMAIL_PATTERN, "[REDACTED:EMAIL]"),
    ("structured_gov_id", _GOV_ID_PATTERN, "[REDACTED:GOV_ID]"),
)

_MAX_INPUT_CHARS = 65_536


@dataclass(frozen=True, slots=True)
class RedactionMatchSummary:
    """Per-rule aggregate match count and safe masked preview."""

    rule: str
    count: int
    sample_masked: str


@dataclass(frozen=True, slots=True)
class RedactionResult:
    """Result of evaluating a prompt payload against redaction rules."""

    allowed: bool
    mode: RedactionMode
    redacted_text: str
    findings: list[RedactionMatchSummary]
    latency_ms: float

    @property
    def total_hits(self) -> int:
        """Return total number of matched entities across all rules."""
        return sum(item.count for item in self.findings)


_PEM_REPLACEMENT = "[REDACTED:PRIVATE_KEY]"


def _redact_pem(text: str) -> tuple[str, int]:
    parts: list[str] = []
    count = 0
    pos = 0
    while (match := _PEM_HEADER.search(text, pos)) is not None:
        parts.extend((text[pos : match.start()], _PEM_REPLACEMENT))
        count += 1
        footer = f"-----END {match[1]}-----"
        end = text.find(footer, match.end())
        if end == -1:
            return "".join(parts), count
        pos = end + len(footer)
    parts.append(text[pos:])
    return "".join(parts), count


def evaluate_redaction(text: str, mode: RedactionMode = "mask") -> RedactionResult:
    """Scan and optionally redact secrets/PII with strict bounded latency."""
    start_ns = time.perf_counter_ns()
    bounded = text[:_MAX_INPUT_CHARS]
    findings: list[RedactionMatchSummary] = []
    transformed, pem_count = _redact_pem(bounded)
    if pem_count:
        findings.append(RedactionMatchSummary("pem_private_key", pem_count, _PEM_REPLACEMENT))

    for rule_name, pattern, replacement in _RULES:
        transformed, count = pattern.subn(replacement, transformed)
        if count:
            findings.append(RedactionMatchSummary(rule_name, count, replacement))

    if mode == "block" and findings:
        allowed = False
        output_text = "[BLOCKED_BY_REDACTION_POLICY]"
    else:
        allowed = True
        output_text = transformed if mode == "mask" else bounded

    elapsed_ms = round((time.perf_counter_ns() - start_ns) / 1_000_000.0, 4)
    return RedactionResult(
        allowed=allowed,
        mode=mode,
        redacted_text=output_text,
        findings=findings,
        latency_ms=elapsed_ms,
    )
