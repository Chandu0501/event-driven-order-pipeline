# Delivery guarantees and tradeoffs

This project deliberately describes its guarantees narrowly. Kafka producer
idempotence, consumer idempotency, and a database transaction do not together
create end-to-end exactly-once processing.

## What the implementation guarantees

- The producer uses `orderId` as its Kafka key, so events for one order are sent
  to the same partition and retain order within that partition.
- Redis `SET NX` atomically claims an event ID before its side effect executes.
  A concurrent delivery cannot acquire the same claim.
- A failed handler releases its claim before rethrowing, allowing Kafka retry.
- A successful handler acknowledges the Kafka record only after its side effect.
- The idempotency window is bounded by a 24-hour TTL. Replays after that window
  are not suppressed.
- A handler must complete inside the TTL. A production implementation should
  use a unique claim token and compare-and-delete release script so an expired,
  reacquired claim cannot be deleted by its previous owner.

## Known dual-write window

`OrderService` writes PostgreSQL and publishes to Kafka from the same call, but
those resources do not share a transaction. A process failure can occur between
the database commit and Kafka publication, and an asynchronous publish failure
can be observed too late to roll back the database transaction.

For a production version, the next design step is a transactional outbox:

1. Store the order change and an outbox row in one PostgreSQL transaction.
2. Relay unpublished outbox rows to Kafka with a stable event ID.
3. Mark rows published only after Kafka acknowledges the send.
4. Rely on the consumer's idempotency claim when the relay republishes after a
   crash between Kafka acknowledgement and the outbox status update.

This favors recoverability and auditability over pretending that a cross-system
exactly-once guarantee exists.
