{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE LambdaCase                #-}
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
  , forEventsWithNumber
  , foldEventsWithNumberM
  , foldEventsM
  , foldEvents
  , forSavedEvents
  , foldSavedEventsM
  , foldSavedEvents
  , foldSubSaved
  , foldSubSavedAsync
  , ForEventFailure(..)
  , unhandled
  ) where

--------------------------------------------------------------------------------
import Control.Monad (MonadPlus, mzero)
import Control.Exception (Exception, SomeException, toException, throwIO)
import Data.Bifunctor (first)
import Data.Foldable (for_)
import Data.Int (Int32)
import Data.Traversable (for)

--------------------------------------------------------------------------------
import Control.Concurrent.Async.Lifted (Async, async, wait)
import Control.Monad.Base (MonadBase, liftBase)
import Control.Monad.Except (ExceptT, runExceptT, mapExceptT, throwError)
import Control.Monad.State (get, put, evalStateT)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Control (MonadBaseControl, StM)
import Data.IORef.Lifted (IORef, newIORef, atomicModifyIORef')
import Data.Text (Text)
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)

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
freshSubscriptionId :: MonadBase IO m => m SubscriptionId
freshSubscriptionId = liftBase $ fmap SubscriptionId nextRandom

--------------------------------------------------------------------------------
-- | A subscription allows to be notified on every change occuring on a stream.
data Subscription =
  Subscription { subscriptionId :: SubscriptionId

               , nextEvent :: forall m. MonadBase IO m
                           => m (Either SomeException SavedEvent)
               }

--------------------------------------------------------------------------------
-- | Waits for the next event and deserializes it on the go.
nextEventAs :: (DecodeEvent a, MonadBase IO m)
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
foldSub :: (DecodeEvent a, MonadBase IO m)
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
foldSubAsync :: (MonadBaseControl IO m, DecodeEvent a)
             => Subscription
             -> (a -> m ())
             -> (SomeException -> m ())
             -> m (Async (StM m ()))
foldSubAsync sub onEvent onError =
  async $ foldSub sub onEvent onError

--------------------------------------------------------------------------------
-- | Similar to 'foldSub' but provides access to the 'SavedEvent' instead of
--   decoded event.
foldSubSaved :: MonadBase IO m
             => Subscription
             -> (SavedEvent -> m ())
             -> (SomeException -> m ())
             -> m ()
foldSubSaved sub onEvent onError = loop
  where
    loop = do
      res <- nextEvent sub
      case res of
        Left e -> onError e
        Right a -> onEvent a >> loop

--------------------------------------------------------------------------------
-- | Asynchronous version of 'foldSubSaved'.
foldSubSavedAsync :: MonadBaseControl IO m
                  => Subscription
                  -> (SavedEvent -> m ())
                  -> (SomeException -> m ())
                  -> m (Async (StM m ()))
foldSubSavedAsync sub onEvent onError =
  async $ foldSubSaved sub onEvent onError

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
  appendEvents :: (EncodeEvent a, MonadBase IO m)
               => store
               -> StreamName
               -> ExpectedVersion
               -> [a]
               -> m (Async EventNumber)

  -- | Appends a batch of events at the end of a stream.
  readBatch :: MonadBase IO m
            => store
            -> StreamName
            -> Batch
            -> m (Async (ReadStatus Slice))

  -- | Subscribes to given stream.
  subscribe :: MonadBase IO m => store -> StreamName -> m Subscription

  -- | Encapsulates to an abstract store.
  toStore :: store -> SomeStore
  toStore = SomeStore

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
appendEvent :: (EncodeEvent a, MonadBase IO m, Store store)
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
instance Exception ForEventFailure

--------------------------------------------------------------------------------
-- | Iterates over all events of stream given a starting point and a batch size.
forEvents :: (MonadBase IO m, DecodeEvent a, Store store)
          => store
          -> StreamName
          -> (a -> m ())
          -> ExceptT ForEventFailure m ()
forEvents store name k = forEventsWithNumber store name (const k)

--------------------------------------------------------------------------------
-- | Iterates over all events of stream given a starting point and a batch size.
--   It also passes the 'EventNumber' at each recursion step.
forEventsWithNumber :: (MonadBase IO m, DecodeEvent a, Store store)
                    => store
                    -> StreamName
                    -> (EventNumber -> a -> m ())
                    -> ExceptT ForEventFailure m ()
forEventsWithNumber store name k = do
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
          Right a -> lift (k (eventNumber saved) a) >> loop i

--------------------------------------------------------------------------------
-- | Like 'forEvents' but expose signature similar to 'foldM'.
foldEventsM :: (MonadBase IO m, DecodeEvent a, Store store)
            => store
            -> StreamName
            -> (s -> a -> m s)
            -> s
            -> ExceptT ForEventFailure m s
foldEventsM store stream k seed =
  foldEventsWithNumberM store stream (const k) seed

--------------------------------------------------------------------------------
-- | Like 'forEvents' but expose signature similar to 'foldM' and also passes
--   the 'EventNumber' at each recursion step.
foldEventsWithNumberM :: (MonadBase IO m, DecodeEvent a, Store store)
            => store
            -> StreamName
            -> (EventNumber -> s -> a -> m s)
            -> s
            -> ExceptT ForEventFailure m s
foldEventsWithNumberM store stream k seed = mapExceptT trans action
  where
    trans m = evalStateT m seed

    action = do
      forEventsWithNumber store stream $ \num a -> do
        s <- get
        s' <- lift $ k num s a
        put s'
      get


--------------------------------------------------------------------------------
-- | Like 'foldEventsM' but expose signature similar to 'foldl'.
foldEvents :: (MonadBase IO m, DecodeEvent a, Store store)
           => store
           -> StreamName
           -> (s -> a -> s)
           -> s
           -> ExceptT ForEventFailure m s
foldEvents store stream k seed =
  foldEventsM store stream (\s a -> return $ k s a) seed

--------------------------------------------------------------------------------
-- | Like `forEvents` but provides access to 'SavedEvent' instead of
--   decoded event.
forSavedEvents :: (MonadBase IO m, Store store)
               => store
               -> StreamName
               -> (SavedEvent -> m ())
               -> ExceptT ForEventFailure m ()
forSavedEvents store name k = do
  res <- streamIterator store name
  case res of
    ReadSuccess i -> loop i
    ReadFailure e -> throwError $ ForEventReadFailure e
  where
    loop i = do
      opt <- iteratorNext i
      for_ opt $ \saved -> lift (k saved) >> loop i

--------------------------------------------------------------------------------
-- | Like 'forSavedEvents' but expose signature similar to 'foldM'.
foldSavedEventsM :: (MonadBase IO m, Store store)
                 => store
                 -> StreamName
                 -> (s -> SavedEvent -> m s)
                 -> s
                 -> ExceptT ForEventFailure m s
foldSavedEventsM store stream k seed = mapExceptT trans action
  where
    trans m = evalStateT m seed

    action = do
      forSavedEvents store stream $ \a -> do
        s <- get
        s' <- lift $ k s a
        put s'
      get

--------------------------------------------------------------------------------
-- | Like 'foldSavedEventsM' but expose signature similar to 'foldl'.
foldSavedEvents :: (MonadBase IO m, Store store)
                => store
                -> StreamName
                -> (s -> SavedEvent -> s)
                -> s
                -> ExceptT ForEventFailure m s
foldSavedEvents store stream k seed =
  foldSavedEventsM store stream (\s a -> return $ k s a) seed

--------------------------------------------------------------------------------
-- | Throws an exception in case 'ExceptT' was a 'Left'.
unhandled :: (MonadBase IO m, Exception e) => ExceptT e m a -> m a
unhandled m = runExceptT m >>= \case
  Left e  -> liftBase $ throwIO e
  Right a -> pure a

--------------------------------------------------------------------------------
-- | Allows to easily iterate over a stream's events.
newtype StreamIterator =
  StreamIterator { iteratorNext :: forall m. MonadBase IO m => m (Maybe SavedEvent) }

--------------------------------------------------------------------------------
instance Show StreamIterator where
  show _ = "StreamIterator"

--------------------------------------------------------------------------------
-- | Reads the next available event from the 'StreamIterator' and try to
--   deserialize it at the same time.
iteratorNextEvent :: (DecodeEvent a, MonadBase IO m, MonadPlus m)
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
iteratorReadAll :: MonadBase IO m => StreamIterator -> m [SavedEvent]
iteratorReadAll i = do
  res <- iteratorNext i
  case res of
    Nothing -> return []
    Just s -> fmap (s:) $ iteratorReadAll i

--------------------------------------------------------------------------------
-- | Like 'iteratorReadAll' but try to deserialize the events at the same time.
iteratorReadAllEvents :: (DecodeEvent a, MonadBase IO m, MonadPlus m)
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
streamIterator :: (Store store, MonadBase IO m)
               => store
               -> StreamName
               -> m (ReadStatus StreamIterator)
streamIterator store name = do
  w <- readBatch store name (startFrom 0)
  res <- liftBase $ wait w
  for res $ \slice -> do
    ref <- newIORef $ IteratorOverAvailable slice
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
iterateOver :: (Store store, MonadBase IO m)
            => store
            -> IORef IteratorOverState
            -> StreamName
            -> m (Maybe SavedEvent)
iterateOver store ref name = go
  where
    go = do
      action <- atomicModifyIORef' ref $ \st ->
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
          res <- liftBase $ wait w
          case res of
            ReadFailure _ -> do
              atomicModifyIORef' ref $ \_ -> (IteratorOverClosed, ())
              return Nothing
            ReadSuccess slice -> do
              let nxtSt = IteratorOverAvailable slice
              atomicModifyIORef' ref $ \_ -> (nxtSt, ())
              go
