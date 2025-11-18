from flask import Flask
import requests

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.propagate import inject

trace.set_tracer_provider(
   TracerProvider(
       resource=Resource.create({SERVICE_NAME: "trigger_app"}),
   )
)
jaeger_exporter = JaegerExporter( #jaeger
   collector_endpoint='http://jaeger:14268/api/traces',
)
trace.get_tracer_provider().add_span_processor(
   BatchSpanProcessor(jaeger_exporter)
)

app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument()

@app.route("/start")
def start():
    with trace.get_tracer(__name__).start_as_current_span("start") as span:
        trace_id = format(span.get_span_context().trace_id, '032x')
        span_id = format(span.get_span_context().span_id, '016x')

        # Создаем headers с контекстом
        headers = {}
        inject(headers)  # W3C Trace Context
        headers['X-Trace-ID'] = trace_id
        headers['X-Span-ID'] = span_id
        headers['X-Request-URI'] = '/start'

        app.logger.info(
            f'Sending request - TraceID: {trace_id}, SpanID: {span_id}')

        # Отправляем с headers
        res = requests.get("http://app:8080/start", headers=headers)
        app.logger.info('application trigger was sent')
    return 'app started'