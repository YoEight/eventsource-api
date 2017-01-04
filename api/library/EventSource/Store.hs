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
module EventSource.Store
  ( Batch(..)
  , Subscription(..)
  , SubscriptionId
  , ExpectedVersionException(..)
  , Store(..)
  , SomeStore(..)
  , StreamIterator
  , iteratorNext
  , iteratorNextEvent
  , iteratorReadAll
  , iteratorReadAllEvents
  , streamIterator
  , freshSubscriptionId
  , startFrom
  , nextEventAs
  , foldSub
  , foldSubAsync
  , appendEvent
  , forEvents
  , foldEventsM
  , foldEvents
  ) where

--------------------------------------------------------------------------------
import Prelude (Show(..))

--------------------------------------------------------------------------------
import Control.Monad.Except
import Data.IORef
import Data.UUID
import Data.UUID.V4
import Protolude hiding (from, show, trans)

--------------------------------------------------------------------------------
import EventSource.Types

--------------------------------------------------------------------------------
-- | Represents batch information needed to read a stream.
data Batch =
  Batch { batchFrom :: EventNumber
        , batchSize :: Int32
        }

--------------------------------------------------------------------------------
-- | Starts a 'Batch' from a given point. The batch size is set to default,
--   which is 500.
startFrom :: EventNumber -> Batch
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
               -> m (Async EventNumber)

  -- | Appends a batch of events at the end of a stream.
  readBatch :: MonadIO m
            => store
            -> StreamName
            -> Batch
            -> m (Async (ReadStatus Slice))

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
            -> m (Async EventNumber)
appendEvent store stream ver a = appendEvents store stream ver [a]

--------------------------------------------------------------------------------
-- | Represents failures that can occurs when using 'forEvents'.
data ForEventFailure
  = ForEventReadFailure ReadFailure
  | ForEventDecodeFailure Text
  deriving Show

--------------------------------------------------------------------------------
-- | Iterates over all events of stream given a starting point and a batch size.
forEvents :: (MonadIO m, DecodeEvent a, Store store)
          => store
          -> StreamName
          -> (a -> m ())
          -> ExceptT ForEventFailure m ()
forEvents store name k = do
  res <- streamIterator store name
  case res of
    ReadSuccess i -> loop i
    ReadFailure e -> throwError $ ForEventReadFailure e
  where
    loop i = do
      opt <- iteratorNext i
      for_ opt $ \saved ->
        case decodeEvent $ savedEvent saved of
          Left e -> throwError $ ForEventDecodeFailure e
          Right a -> lift (k a) >> loop i

--------------------------------------------------------------------------------
-- | Like 'forEvents' but expose signature similar to 'foldM'.
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
-- | Like 'foldEventsM' but expose signature similar to 'foldl'.
foldEvents :: (MonadIO m, DecodeEvent a, Store store)
           => store
           -> StreamName
           -> (s -> a -> s)
           -> s
           -> ExceptT ForEventFailure m s
foldEvents store stream k seed =
  foldEventsM store stream (\s a -> return $ k s a) seed

--------------------------------------------------------------------------------
-- | Allows to easily iterate over a stream's events.
newtype StreamIterator =
  StreamIterator { iteratorNext :: forall m. MonadIO m => m (Maybe SavedEvent) }

--------------------------------------------------------------------------------
instance Show StreamIterator where
  show _ = "StreamIterator"

--------------------------------------------------------------------------------
-- | Reads the next available event from the 'StreamIterator' and try to
--   deserialize it at the same time.
iteratorNextEvent :: (DecodeEvent a, MonadIO m, MonadPlus m)
                  => StreamIterator
                  -> m (Maybe a)
iteratorNextEvent i = do
  res <- iteratorNext i
  case res of
    Nothing -> return Nothing
    Just s ->
      case decodeEvent $ savedEvent s of
        Left _ -> mzero
        Right a -> return $ Just a

--------------------------------------------------------------------------------
-- | Reads all events from the 'StreamIterator' until reaching end of stream.
iteratorReadAll :: MonadIO m => StreamIterator -> m [SavedEvent]
iteratorReadAll i = do
  res <- iteratorNext i
  case res of
    Nothing -> return []
    Just s -> fmap (s:) $ iteratorReadAll i

--------------------------------------------------------------------------------
-- | Like 'iteratorReadAll' but try to deserialize the events at the same time.
iteratorReadAllEvents :: (DecodeEvent a, MonadIO m, MonadPlus m)
                      => StreamIterator
                      -> m [a]
iteratorReadAllEvents i = do
  res <- iteratorNextEvent i
  case res of
    Nothing -> return []
    Just a -> fmap (a:) $ iteratorReadAllEvents i

--------------------------------------------------------------------------------
-- | Returns a 'StreamIterator' for the given stream name. The returned
--   'StreamIterator' IS NOT THREADSAFE.
streamIterator :: (Store store, MonadIO m)
               => store
               -> StreamName
               -> m (ReadStatus StreamIterator)
streamIterator store name = do
  w <- readBatch store name (startFrom 0)
  res <- liftIO $ wait w
  for res $ \slice -> do
    ref <- liftIO $ newIORef $ IteratorOverAvailable slice
    return $ StreamIterator $ iterateOver store ref name

--------------------------------------------------------------------------------
data IteratorOverState
  = IteratorOverAvailable Slice
  | IteratorOverClosed

--------------------------------------------------------------------------------
data IteratorOverAction
  = IteratorOverEvent SavedEvent
  | IteratorOverNextBatch EventNumber
  | IteratorOverEndOfStream

--------------------------------------------------------------------------------
iterateOver :: (Store store, MonadIO m)
            => store
            -> IORef IteratorOverState
            -> StreamName
            -> m (Maybe SavedEvent)
iterateOver store ref name = go
  where
    go = do
      action <- liftIO $ atomicModifyIORef' ref $ \st ->
        case st of
          IteratorOverAvailable slice ->
            case sliceEvents slice of
              e:es ->
                let nextSlice = slice { sliceEvents = es }
                    nxtSt = IteratorOverAvailable nextSlice in
                (nxtSt, IteratorOverEvent e)
              [] | sliceEndOfStream slice
                   -> (IteratorOverClosed, IteratorOverEndOfStream)
                 | otherwise
                   -> let resp = IteratorOverNextBatch $
                                 sliceNextEventNumber slice in
                     (st, resp)
          IteratorOverClosed -> (st, IteratorOverEndOfStream)

      case action of
        IteratorOverEvent e -> return $ Just e
        IteratorOverEndOfStream -> return Nothing
        IteratorOverNextBatch num -> do
          w <- readBatch store name (startFrom num)
          res <- liftIO $ wait w
          case res of
            ReadFailure _ -> do
              liftIO $ atomicModifyIORef' ref $ \_ -> (IteratorOverClosed, ())
              return Nothing
            ReadSuccess slice -> do
              let nxtSt = IteratorOverAvailable slice
              liftIO $ atomicModifyIORef' ref $ \_ -> (nxtSt, ())
              go
