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
  Subscription { nextEvent :: forall a m. (DecodeEvent a, MonadIO m)
                           => m (Either SomeException a) }

--------------------------------------------------------------------------------
-- | Folds over every event coming from the subscription until the end of the
--   universe, unless an 'Exception' raises from the subscription.
--   'SomeException' is used because we let the underlying subscription model
--   exposed its own 'Exception'. If the callback that handles incoming events
--   throws an exception, it will not be catch by the error callback.
foldSub :: (DecodeEvent a, MonadIO m)
        => Subscription
        -> (a -> m ())
        -> (SomeException -> m ())
        -> m ()
foldSub sub onEvent onError = loop
  where
    loop = do
      res <- nextEvent sub
      case res of
        Left e -> onError e
        Right a -> onEvent a >> loop

--------------------------------------------------------------------------------
-- | Asynchronous version of 'foldSub'.
foldSubAsync :: DecodeEvent a
             => Subscription
             -> (a -> IO ())
             -> (SomeException -> IO ())
             -> IO (Async ())
foldSubAsync sub onEvent onError =
  async $ foldSub sub onEvent onError

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
-- | Appends a single event at the end of a stream.
appendEvent :: (EncodeEvent a, MonadIO m)
            => Store
            -> StreamName
            -> ExpectedVersion
            -> a
            -> m ()
appendEvent store stream ver a = appendEvents store stream ver [a]

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

--------------------------------------------------------------------------------
-- | Like 'forEvents' but expose signature similar to 'foldl'.
foldEventsM :: (MonadIO m, DecodeEvent a)
            => Store
            -> StreamName
            -> (s -> a -> m s)
            -> s
            -> ExceptT ForEventFailure m s
foldEventsM store stream k seed = mapExceptT trans action
  where
    trans m = evalStateT m seed

    action = do
      forEvents store stream $ \a -> do
        s <- get
        s' <- lift $ k s a
        put s'
      get

--------------------------------------------------------------------------------
-- | Like 'foldEventsM' but expose signature similar to 'foldM'.
foldEvents :: (MonadIO m, DecodeEvent a)
           => Store
           -> StreamName
           -> (s -> a -> s)
           -> s
           -> ExceptT ForEventFailure m s
foldEvents store stream k seed =
  foldEventsM store stream (\s a -> return $ k s a) seed
