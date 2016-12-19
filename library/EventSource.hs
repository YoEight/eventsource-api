{-# LANGUAGE Rank2Types #-}
--------------------------------------------------------------------------------
-- |
-- Module : EventSource
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module EventSource where

--------------------------------------------------------------------------------
import ClassyPrelude
import Control.Monad.State

--------------------------------------------------------------------------------
import EventSource.Types

--------------------------------------------------------------------------------
-- | Represents a range of value given a starting point to end one.
data Range =
  Range { rangeFrom :: Int32
        , rangeTo :: Int32
        }

--------------------------------------------------------------------------------
-- | Main event store abstraction. It exposes essential features expected from
--   an event store.
data Store =
  Store { appendEvents :: forall a m. (EncodeEvent a, MonadIO m)
                       => StreamName
                       -> ExpectedVersion
                       -> [a]
                       -> m ()
          -- ^ Appends a batch of events at the end of a stream.

        , readBatch :: forall m. MonadIO m
                    => StreamName
                    -> Range
                    -> m (ReadStatus Slice)
          -- ^ Reads a batch of events, in a forward direction, on a stream.

        }

--------------------------------------------------------------------------------
forEvents :: (MonadState s m, MonadIO m, DecodeEvent a)
          => Store
          -> StreamName
          -> (a -> m ())
          -> m ()
forEvents = undefined
