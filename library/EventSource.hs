{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE Rank2Types       #-}
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
import Control.Monad.Except
import Control.Monad.State

--------------------------------------------------------------------------------
import EventSource.Types

--------------------------------------------------------------------------------
-- | Represents batch information needed to read a stream.
data Batch =
  Batch { batchFrom :: Int32
        , batchSize :: Int32
        }

--------------------------------------------------------------------------------
-- | Starts a 'Batch' from a given point. The batch size is set to default,
--   which is 500.
startFrom :: Int32 -> Batch
startFrom from = Batch from 500

--------------------------------------------------------------------------------
-- | A subscription allows to be notified on every change occuring on a stream.
newtype Subscription =
  Subscription { foldSub :: forall a m. (DecodeEvent a, MonadIO m)
                         => (a -> m ())
                         -> m () }

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
                    -> Batch
                    -> m (ReadStatus Slice)
          -- ^ Reads a batch of events, in a forward direction, on a stream.

        , subscribe :: forall m. MonadIO m => StreamName -> m Subscription
          -- ^ Subscribes to given stream.

        }

--------------------------------------------------------------------------------
-- | Represents failures that can occurs when using 'forEvents'.
data ForEventFailure
  = ForEventReadFailure ReadFailure
  | ForEventDecodeFailure Text

--------------------------------------------------------------------------------
-- | Iterates over all events of stream given a starting point and a batch size.
forEvents :: (MonadState s m, MonadIO m, DecodeEvent a)
          => Store
          -> StreamName
          -> (a -> m ())
          -> ExceptT ForEventFailure m ()
forEvents store stream k = loop $ startFrom 0
  where
    loop batch = do
      res <- readBatch store stream batch
      case res of
        ReadSuccess slice -> do
          for_ (sliceEvents slice) $ \s ->
            case decodeEvent $ savedEvent s of
              Left e -> throwError $ ForEventDecodeFailure e
              Right a -> lift $ k a

          let nextBatch = batch { batchFrom = sliceNextEventNumber slice }

          if sliceEndOfStream slice
            then return ()
            else loop nextBatch
        ReadFailure e -> throwError $ ForEventReadFailure e
