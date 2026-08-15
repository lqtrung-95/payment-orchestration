-- Trace context carried through the outbox.
--
-- The transactional outbox deliberately decouples the request from the publish:
-- the HTTP handler commits a row and returns, and a relay picks it up later in
-- a different goroutine. That decoupling is the whole point of the pattern, and
-- it is also where a distributed trace breaks — there is no in-process context
-- linking the two, so the publish looks like work nobody asked for.
--
-- Storing the W3C traceparent alongside the message is the same trick used to
-- carry it through Kafka headers, applied to the other boundary. Without it a
-- trace starts at the relay and the request that caused the payment is missing
-- from its own story.
ALTER TABLE outbox
    ADD COLUMN traceparent TEXT;
