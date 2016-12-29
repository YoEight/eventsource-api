# [eventsource-api][]

This project provides what we think be a lean eventsourcing API. The goal is to
set a common ground for eventsourcing-based application, yet doesn't force you
to be trapped under a restrictive framework.

This library main abstraction is the `Store` typeclass.

```haskell
-- | Main event store abstraction. It exposes essential features expected from
--   an event store.
class Store store where
  -- | Appends a batch of events at the end of a stream.
  appendEvents :: (EncodeEvent a, MonadIO m)
               => store
               -> StreamName
               -> ExpectedVersion
               -> [a]
               -> m (Async EventNumber)

  -- | Appends a batch of events at the end of a stream.
  readBatch :: MonadIO m
            => store
            -> StreamName
            -> Batch
            -> m (Async (ReadStatus Slice))

  -- | Subscribes to given stream.
  subscribe :: MonadIO m => store -> StreamName -> m Subscription
```

The idea is to use any backend of your liking as long as it's able to derive that typeclass with its related implicit constraints.

  1. `appendEvents` MUST add events at the end of a stream without doing any other alteration on the stream. The order of the event's array MUST be respected within the store. Most important, the implementation MUST comply to the given `ExpectedVersion` passed by the user.
  
  2. `readBatch` SHOULD retrieve a set of event given a starting position and a batch size (represented by the `Batch` parameter). The returned array MUST have its events ordered by their `EventNumber` property.

  3. `subscribe` SHOULD register a subscription to the given stream. An implementation MUST allow to subscribe to a stream that doesn't exist yet. A subscription allows the user to be notified of any event added to the stream. There is no mandatory timing to meet regarding how fast the subscription is notified by a change. We only suggest to dispatch new events as soon as possible.

#### `ExpectedVersion`

A word on `ExpectedVersion`. It's a mean to preserve data consistency in a concurrent setting.

```haskell
-- | The purpose of 'ExpectedVersion' is to make sure a certain stream state is
--   at an expected point in order to carry out a write.
data ExpectedVersion
  = AnyVersion
    -- Stream is a any given state.
  | NoStream
    -- Stream shouldn't exist.
  | StreamExists
    -- Stream should exist.
  | ExactVersion EventNumber
    -- Stream should be at givent event number.
```

On a write, if the `ExpectedVersion` condition given by the user is not met within the store, the implementation should raise an exception. At the moment, the API doesn't capture this situation in the type system.

[eventsource-api]: https://github.com/YoEight/eventsource-api
