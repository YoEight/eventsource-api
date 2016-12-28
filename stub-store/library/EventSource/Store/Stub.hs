{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
--------------------------------------------------------------------------------
-- |
-- Module : EventSource.Store.Stub
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
-- This module exposes an implementation of Store for testing purpose.
-- This implementation is threadsafe.
--------------------------------------------------------------------------------
module EventSource.Store.Stub
  ( Stream(..)
  , StubStore
  , newStub
  , streams
  , subscriptionIds
  , lastStreamEvent
  ) where

--------------------------------------------------------------------------------
import ClassyPrelude
import Control.Monad.State.Strict
import Data.Semigroup
import Data.Sequence hiding (filter, zip)

--------------------------------------------------------------------------------
import EventSource.Store
import EventSource.Types hiding (singleton)

--------------------------------------------------------------------------------
-- | Holds stream state data.
data Stream =
  Stream { streamNextNumber :: EventNumber
         , streamEvents :: Seq SavedEvent
         }

--------------------------------------------------------------------------------
type Sub = TChan SavedEvent
type Subs = Map SubscriptionId Sub

--------------------------------------------------------------------------------
data StubStore =
  StubStore { _streams :: TVar (Map StreamName Stream)
            , _subs :: TVar (Map StreamName Subs)
            }

--------------------------------------------------------------------------------
-- | Creates a new stub event store.
newStub :: IO StubStore
newStub = StubStore <$> newTVarIO mempty <*> newTVarIO mempty

--------------------------------------------------------------------------------
-- | Returns current 'StubStore' streams state.
streams :: StubStore -> IO (Map StreamName Stream)
streams StubStore{..} = readTVarIO _streams

--------------------------------------------------------------------------------
-- | Returns the last event of stream.
lastStreamEvent :: StubStore -> StreamName -> IO (Maybe SavedEvent)
lastStreamEvent stub name = do
  streamMap <- streams stub
  return (go =<< lookup name streamMap)
    where
      go stream =
        case viewr $ streamEvents stream of
          EmptyR -> Nothing
          _ :> e -> Just e

--------------------------------------------------------------------------------
-- | Returns all subscriptions a stream has.
subscriptionIds :: StubStore -> StreamName -> IO [SubscriptionId]
subscriptionIds StubStore{..} name = do
  subMap <- readTVarIO _subs
  case lookup name subMap of
    Nothing -> return []
    Just subs -> return $ keys subs

--------------------------------------------------------------------------------
appendStream :: [Event] -> Stream -> Stream
appendStream = flip $ foldl' go
  where
    go s e =
      let num = streamNextNumber s
          evts = streamEvents s in
      s { streamNextNumber = num + 1
        , streamEvents = snoc evts (SavedEvent num e)
        }

--------------------------------------------------------------------------------
newStream :: [Event] -> Stream
newStream xs = appendStream xs (Stream 0 mempty)

--------------------------------------------------------------------------------
notifySubs :: StubStore -> StreamName -> [SavedEvent] -> STM ()
notifySubs StubStore{..} name events = do
  subMap <- readTVar _subs
  for_ (lookup name subMap) $ \subs ->
    for_ subs $ \sub ->
      for_ events $ \e ->
        writeTChan sub e

--------------------------------------------------------------------------------
buildEvent :: (EncodeEvent a, MonadIO m) => a -> m Event
buildEvent a = do
  eid <- freshEventId
  let start = Event { eventType = ""
                    , eventId = eid
                    , eventPayload = dataFromBytes ""
                    , eventMetadata = Nothing
                    }

  return $ execState (encodeEvent a) start

--------------------------------------------------------------------------------
instance Store StubStore where
  appendEvents self@StubStore{..} name ver xs = do
    events <- traverse buildEvent xs
    liftIO $ async $ atomically $ do
      streamMap <- readTVar _streams

      case lookup name streamMap of
        Nothing -> do
          case ver of
            StreamExists ->
              throwSTM $ ExpectedVersionException ver NoStream
            ExactVersion v ->
              unless (v == 0) $ throwSTM
                              $ ExpectedVersionException ver NoStream
            _ -> return ()

          let _F Nothing  = Just $ newStream events
              _F (Just s) = Just $ appendStream events s

              newStreamMap = alterMap _F name streamMap

          writeTVar _streams newStreamMap

          -- This part is already performed in 'appendStream' but difficult
          -- to take its logic apart from building 'SavedEvent's.
          let saved = uncurry SavedEvent <$> zip [0..] events
          notifySubs self name saved
          let last = getLast $ ofoldMap1Ex Last saved
              nextNum = eventNumber last + 1
          return nextNum


        Just stream -> do
          let currentNumber = streamNextNumber stream
          case ver of
            NoStream ->
              throwSTM $ ExpectedVersionException ver StreamExists
            ExactVersion v ->
              unless (v == streamNextNumber stream - 1)
                $ throwSTM
                $ ExpectedVersionException ver (ExactVersion currentNumber)

            _ -> return ()

          let nextStream = appendStream events stream
              newStreamMap = adjustMap (const nextStream) name streamMap

          writeTVar _streams newStreamMap

          -- This part is already performed in 'appendStream' but difficult
          -- to take its logic apart from building 'SavedEvent's.
          let saved = uncurry SavedEvent <$> zip [currentNumber..] events
          notifySubs self name saved
          let last = getLast $ ofoldMap1Ex Last saved
              nextNum = eventNumber last + 1
          return nextNum


  readBatch StubStore{..} name (Batch from _) = liftIO $ async $ atomically $ do
    streamMap <- readTVar _streams
    case lookup name streamMap of
      Nothing -> return $ ReadFailure StreamNotFound
      Just stream -> do
        let events = filter ((>= from) . eventNumber) $ streamEvents stream
            slice = Slice { sliceEvents = toList events
                          , sliceEndOfStream = True
                          , sliceNextEventNumber = streamNextNumber stream
                          }

        return $ ReadSuccess slice

  subscribe StubStore{..} name = do
    sid <- freshSubscriptionId
    atomically $ do
      chan <- newTChan
      let sub = Subscription sid $ atomically $ do
            saved <- readTChan chan
            return $ Right saved

      subMap <- readTVar _subs
      let _F Nothing  = Just $ singletonMap sid  chan
          _F (Just m) = Just $ insertMap sid chan m

          nextSubMap = alterMap _F name subMap

      writeTVar _subs nextSubMap
      return sub
