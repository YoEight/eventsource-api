{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE Rank2Types                #-}
--------------------------------------------------------------------------------
-- |
-- Module : EventSource.Store
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module EventSource.Store where

--------------------------------------------------------------------------------
import ClassyPrelude
import Control.Monad.Except
import Control.Monad.State
import Data.UUID
import Data.UUID.V4

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
-- | Represents a subscription id.
newtype SubscriptionId = SubscriptionId UUID deriving (Eq, Ord, Show)

--------------------------------------------------------------------------------
-- | Returns a fresh subscription id.
freshSubscriptionId :: MonadIO m => m SubscriptionId
freshSubscriptionId = liftIO $ fmap SubscriptionId nextRandom

--------------------------------------------------------------------------------
-- | A subscription allows to be notified on every change occuring on a stream.
data Subscription =
  Subscription { subscriptionId :: SubscriptionId

               , nextEvent :: forall m. MonadIO m
                           => m (Either SomeException SavedEvent)
               }

--------------------------------------------------------------------------------
-- | Waits for the next event and deserializes it on the go.
nextEventAs :: (DecodeEvent a, MonadIO m)
            => Subscription
            -> m (Either SomeException a)
nextEventAs sub = do
  res <- nextEvent sub
  let action = do
        event <- res
        first (toException . DecodeEventException)
          $ decodeEvent
          $ savedEvent event

  return action

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
      res <- nextEventAs sub
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
data ExpectedVersionException
  = ExpectedVersionException
    { versionExceptionExpected :: ExpectedVersion
    , versionExceptionActual :: ExpectedVersion
    } deriving Show

--------------------------------------------------------------------------------
instance Exception ExpectedVersionException

--------------------------------------------------------------------------------
-- | Main event store abstraction. It exposes essential features expected from
--   an event store.
class Store store where
  -- | Appends a batch of events at the end of a stream.
  appendEvents :: (EncodeEvent a, MonadIO m)
               => store
               -> StreamName
               -> ExpectedVersion
               -> [a]
               -> m ()

  -- | Appends a batch of events at the end of a stream.
  readBatch :: MonadIO m
            => store
            -> StreamName
            -> Batch
            -> m (ReadStatus Slice)

  -- | Subscribes to given stream.
  subscribe :: MonadIO m => store -> StreamName -> m Subscription

--------------------------------------------------------------------------------
-- | Utility type to pass any store that implements 'Store' typeclass.
data SomeStore = forall store. Store store => SomeStore store

--------------------------------------------------------------------------------
instance Store SomeStore where
  appendEvents (SomeStore store) = appendEvents store
  readBatch (SomeStore store)    = readBatch store
  subscribe (SomeStore store)    = subscribe store

--------------------------------------------------------------------------------
-- | Appends a single event at the end of a stream.
appendEvent :: (EncodeEvent a, MonadIO m, Store store)
            => store
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
forEvents :: (MonadIO m, DecodeEvent a, Store store)
          => store
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
foldEventsM :: (MonadIO m, DecodeEvent a, Store store)
            => store
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
foldEvents :: (MonadIO m, DecodeEvent a, Store store)
           => store
           -> StreamName
           -> (s -> a -> s)
           -> s
           -> ExceptT ForEventFailure m s
foldEvents store stream k seed =
  foldEventsM store stream (\s a -> return $ k s a) seed
