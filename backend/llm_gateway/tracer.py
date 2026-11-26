"""OpenTelemetry-compatible JSON tracer and replayable event recorder."""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class TraceSpanRecord:
    """Single lightweight OpenTelemetry-compatible span."""

    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    attributes: dict[str, Any] = field(default_factory=dict)
    status_code: str = "OK"

    def to_otlp_span(self) -> dict[str, Any]:
        """Serialize span into OTLP JSON span representation."""
        otlp_attrs = []
        for key, value in self.attributes.items():
            if isinstance(value, bool):
                val_obj: dict[str, Any] = {"boolValue": value}
            elif isinstance(value, int):
                val_obj = {"intValue": str(value)}
            elif isinstance(value, float):
                val_obj = {"doubleValue": value}
            else:
                val_obj = {"stringValue": str(value)}
            otlp_attrs.append({"key": key, "value": val_obj})

        return {
            "traceId": self.trace_id,
            "spanId": self.span_id,
            "parentSpanId": self.parent_span_id or "",
            "name": self.name,
            "kind": 2,
            "startTimeUnixNano": str(self.start_time_unix_nano),
            "endTimeUnixNano": str(self.end_time_unix_nano),
            "attributes": otlp_attrs,
            "status": {"code": 1 if self.status_code == "OK" else 2, "message": self.status_code},
        }


class GatewayTraceCollector:
    """Collects per-run OTLP spans in memory for retrieval and SSE streaming."""

    def __init__(self, service_name: str = "llm-quota-throttle-gateway") -> None:
        self._service_name = service_name

    @staticmethod
    def new_trace_id() -> str:
        """Generate a 32-char lowercase hex OTLP traceId."""
        return secrets.token_hex(16)

    @staticmethod
    def new_span_id() -> str:
        """Generate a 16-char lowercase hex OTLP spanId."""
        return secrets.token_hex(8)

    def build_span(
        self,
        *,
        trace_id: str,
        name: str,
        duration_ns: int = 150_000,
        parent_span_id: str | None = None,
        attributes: dict[str, Any] | None = None,
        status_code: str = "OK",
    ) -> TraceSpanRecord:
        """Create a deterministic span timestamped in monotonic-anchored unix nanos."""
        end_ns = time.time_ns()
        start_ns = max(0, end_ns - max(1, duration_ns))
        return TraceSpanRecord(
            trace_id=trace_id,
            span_id=self.new_span_id(),
            parent_span_id=parent_span_id,
            name=name,
            start_time_unix_nano=start_ns,
            end_time_unix_nano=end_ns,
            attributes=attributes or {},
            status_code=status_code,
        )

    def to_otlp_payload(
        self, run_id: str, trace_id: str, spans: list[TraceSpanRecord]
    ) -> dict[str, Any]:
        """Serialize collected spans into canonical OTLP resourceSpans payload."""
        return {
            "run_id": run_id,
            "trace_id": trace_id,
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {"stringValue": self._service_name},
                            }
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "llm_gateway.tracer", "version": "0.1.0"},
                            "spans": [span.to_otlp_span() for span in spans],
                        }
                    ],
                }
            ],
        }
