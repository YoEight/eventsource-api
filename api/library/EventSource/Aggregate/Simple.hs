{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TypeFamilies           #-}
--------------------------------------------------------------------------------
-- |
-- Module    :  EventSource.Aggregate.Simple
-- Copyright :  (C) 2017 Yorick Laupa
-- License   :  (see the file LICENSE)
-- Maintainer:  Yorick Laupa <yo.eight@gmail.com>
-- Stability :  experimental
-- Portability: non-portable
--
--------------------------------------------------------------------------------
module EventSource.Aggregate.Simple
  ( AggIO
  , AggregateIO(..)
  , ValidateIO(..)
  , Simple
  , newAgg
  , loadAgg
  , loadOrCreateAgg
  , submitCmd
  , submitEvt
  , snapshot
  ) where

--------------------------------------------------------------------------------
import Control.Exception (SomeException)

--------------------------------------------------------------------------------
import qualified EventSource.Aggregate as Self
import           EventSource

--------------------------------------------------------------------------------
-- | Simple aggregate abstraction.
newtype Simple id command event state = Simple state

--------------------------------------------------------------------------------
-- | A stream aggregate. An aggregate updates its internal based on the event
--   it receives. You can read its current state by using 'snapshot'. If it
--   supports validation, through 'Validated' typeclass, it can receive
--   command and emits an event if the command was successful. Otherwise, it will
--   yield an error. When receiving valid command, an aggregate will persist the
--   resulting event. An aggregate is only responsible of its own stream.
type AggIO id command event state = Self.Agg (Simple id command event state)

--------------------------------------------------------------------------------
-- | Represents a stream aggregate. An aggregate can rebuild its internal state
--   by replaying all the stream's events that aggregate is responsible for.
class AggregateIO event state | event -> state where

  -- | Given current aggregate state, updates it according to the event the
  --   aggregate receives.
  applyIO :: state -> event -> IO state

--------------------------------------------------------------------------------
-- | Represents an aggregate that support validation. An aggregate that supports
--   validation can receive command and decide if it was valid or not. When the
--   validation is successful, The aggregate emits an event that will be
--   persisted and pass to 'apply' function.
class AggregateIO event state => ValidateIO command event state | command -> state, command -> event where

  -- | Validates a command. If the command validation succeeds, it will emits
  --   an event. Otherwise, it will returns an error.
  validateIO :: state -> command -> IO (Either SomeException event)

--------------------------------------------------------------------------------
instance AggregateIO event state => Self.Aggregate (Simple id command event state) where
  type Id  (Simple id command event state) = id
  type Evt (Simple id command event state) = event
  type M   (Simple id command event state) = IO

  apply (Simple a) e = fmap Simple (applyIO a e)

--------------------------------------------------------------------------------
instance ValidateIO command event state => Self.Validate (Simple id command event state) where
  type Cmd (Simple id command event state) = command
  type Err (Simple id command event state) = SomeException

  validate (Simple a) cmd = validateIO a cmd

--------------------------------------------------------------------------------
-- | Creates a new aggregate given an eventstore handle, an id and an initial
--   state.
newAgg :: AggregateIO event state
       => SomeStore
       -> id
       -> state
       -> IO (AggIO id command event state)
newAgg store aId seed = Self.newAgg store aId (Simple seed)

--------------------------------------------------------------------------------
-- | Creates an aggregate and replays its entire stream to rebuild its
--   internal state.
loadAgg :: (AggregateIO event state, Self.StreamId id, DecodeEvent event)
        => SomeStore
        -> id
        -> state
        -> IO (Either ForEventFailure (AggIO id command event state))
loadAgg store aId seed = Self.loadAgg store aId (Simple seed)

--------------------------------------------------------------------------------
-- | Like 'loadAgg' but call 'loadAgg' in case of 'ForEventFailure' error.
loadOrCreateAgg :: (AggregateIO event state, Self.StreamId id, DecodeEvent event)
                => SomeStore
                -> id
                -> state
                -> IO (AggIO id command event state)
loadOrCreateAgg store aId seed = Self.loadOrCreateAgg store aId (Simple seed)

--------------------------------------------------------------------------------
-- | Submits a command to the aggregate. If the command was valid, it returns
--   an event otherwise an error. In case of a valid command, the aggregate
--   persist the resulting event to the eventstore. The aggregate will also
--   update its internal state accordingly.
submitCmd :: (ValidateIO command event state, Self.StreamId id, EncodeEvent event)
          => AggIO id command event state
          -> command
          -> IO (Either SomeException event)
submitCmd agg cmd = Self.submitCmd agg cmd

--------------------------------------------------------------------------------
-- | Submits an event. The aggregate will update its internal state accondingly.
submitEvt :: AggregateIO event state
          => AggIO id command event state
          -> event
          -> IO ()
submitEvt agg event = Self.submitEvt agg event

--------------------------------------------------------------------------------
-- | Returns current aggregate state.
snapshot :: AggIO id command event state -> IO state
snapshot agg = do
  Simple a <- Self.snapshot agg
  pure a
