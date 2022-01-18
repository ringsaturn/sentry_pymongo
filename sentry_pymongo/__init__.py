from sentry_sdk import Hub


from pymongo.monitoring import CommandListener, CommandStartedEvent, CommandFailedEvent, CommandSucceededEvent


class PyMongoMonitorForSentry(CommandListener):
    _scope = {}

    def started(self, event: CommandStartedEvent):
        if not event.command:
            return

        description = event.command_name

        hub = Hub.current
        with hub.start_span(op="mongo", description=description) as span:
            self._scope[event.request_id] = {
                "trace_id": span.trace_id,
                "parent_span_id": span.parent_span_id,
            }
            span.set_tag("mongo.database_name", event.database_name)
            span.set_tag("mongo.request_id", event.request_id)
            span.set_data("mongo.commond", event.command)

    def succeeded(self, event: CommandSucceededEvent):
        cache = self._scope.pop(event.request_id)
        if cache is None:
            return
        try:
            trace_id = cache["trace_id"]
            parent_span_id = cache["parent_span_id"]
        except Exception:
            pass
        hub = Hub.current
        with hub.start_span(trace_id=trace_id, parent_span_id=parent_span_id) as span:
            span.set_tag("mongo.operation_id", event.operation_id)

    def failed(self, event: CommandFailedEvent):
        cache = self._scope.pop(event.request_id)
        if cache is None:
            return
        try:
            trace_id = cache["trace_id"]
            parent_span_id = cache["parent_span_id"]
        except Exception:
            pass
        hub = Hub.current
        with hub.start_span(trace_id=trace_id, parent_span_id=parent_span_id) as span:
            span.set_tag("mongo.operation_id", event.operation_id)
