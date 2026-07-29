ALTER TABLE webhook_deliveries
    ADD COLUMN traceparent text,
    ADD COLUMN tracestate text;
