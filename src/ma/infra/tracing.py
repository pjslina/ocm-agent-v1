"""OpenTelemetry SDK 初始化。

调用一次 `configure_tracing(settings)`，之后业务代码用：
    from opentelemetry import trace
    tracer = trace.get_tracer("ma.<module>")
"""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from ma.infra.settings import Settings


def configure_tracing(settings: Settings) -> None:
    """初始化全局 TracerProvider。

    无 OTLP endpoint 时仍设置 provider（spans 进内存即被丢弃），
    便于本地开发不依赖 collector 也能跑。
    """
    resource = Resource.create(
        {
            "service.name": settings.otel_service_name,
            "service.environment": settings.env,
        }
    )
    provider = TracerProvider(resource=resource)
    if settings.otel_exporter_otlp_endpoint:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint))
        )
    trace.set_tracer_provider(provider)

    # M3: 接入 LangChain instrumentation（自动为 LLM 调用 + LangGraph 节点创建 spans）
    try:
        from openinference.instrumentation.langchain import LangChainInstrumentor

        LangChainInstrumentor().instrument()
    except ImportError:
        pass
