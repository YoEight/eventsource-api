{-# LANGUAGE FlexibleContexts  #-}
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
import Control.Monad (unless)
import           Control.Concurrent.STM (STM)
import qualified Control.Concurrent.STM as STM
import Data.Foldable (toList, for_, foldl')
import Data.Monoid (Last(..))

--------------------------------------------------------------------------------
import           Control.Concurrent.Async (async)
import           Control.Monad.Base (MonadBase, liftBase)
import           Control.Monad.State.Strict (execState)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Sequence (Seq, (|>))
import qualified Data.Sequence as S

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
type Sub = STM.TChan SavedEvent
type Subs = Map SubscriptionId Sub

--------------------------------------------------------------------------------
data StubStore =
  StubStore { _streams :: STM.TVar (Map StreamName Stream)
            , _subs :: STM.TVar (Map StreamName Subs)
            }

--------------------------------------------------------------------------------
-- | Creates a new stub event store.
newStub :: IO StubStore
newStub = StubStore <$> STM.newTVarIO mempty <*> STM.newTVarIO mempty

--------------------------------------------------------------------------------
-- | Returns current 'StubStore' streams state.
streams :: StubStore -> IO (Map StreamName Stream)
streams StubStore{..} = STM.readTVarIO _streams

--------------------------------------------------------------------------------
-- | Returns the last event of stream.
lastStreamEvent :: StubStore -> StreamName -> IO (Maybe SavedEvent)
lastStreamEvent stub name = do
  streamMap <- streams stub
  return (go =<< Map.lookup name streamMap)
    where
      go stream =
        case S.viewr $ streamEvents stream of
          S.EmptyR -> Nothing
          _ S.:> e -> Just e

--------------------------------------------------------------------------------
-- | Returns all subscriptions a stream has.
subscriptionIds :: StubStore -> StreamName -> IO [SubscriptionId]
subscriptionIds StubStore{..} name = do
  subMap <- STM.readTVarIO _subs
  case Map.lookup name subMap of
    Nothing -> return []
    Just subs -> return $ Map.keys subs

--------------------------------------------------------------------------------
appendStream :: [Event] -> Stream -> Stream
appendStream = flip $ foldl' go
  where
    go s e =
      let num = streamNextNumber s
          evts = streamEvents s in
      s { streamNextNumber = num + 1
        , streamEvents = evts |> SavedEvent num e Nothing
        }

--------------------------------------------------------------------------------
newStream :: [Event] -> Stream
newStream xs = appendStream xs (Stream 0 mempty)

--------------------------------------------------------------------------------
notifySubs :: StubStore -> StreamName -> [SavedEvent] -> STM ()
notifySubs StubStore{..} name events = do
  subMap <- STM.readTVar _subs
  for_ (Map.lookup name subMap) $ \subs ->
    for_ subs $ \sub ->
      for_ events $ \e ->
        STM.writeTChan sub e

--------------------------------------------------------------------------------
buildEvent :: (EncodeEvent a, MonadBase IO m) => a -> m Event
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
    liftBase $ async $ STM.atomically $ do
      streamMap <- STM.readTVar _streams

      case Map.lookup name streamMap of
        Nothing -> do
          case ver of
            StreamExists ->
              STM.throwSTM $ ExpectedVersionException ver NoStream
            ExactVersion v ->
              unless (v == 0) $ STM.throwSTM
                              $ ExpectedVersionException ver NoStream
            _ -> return ()

          let _F Nothing  = Just $ newStream events
              _F (Just s) = Just $ appendStream events s

              newStreamMap = Map.alter _F name streamMap

          STM.writeTVar _streams newStreamMap

          -- This part is already performed in 'appendStream' but difficult
          -- to take its logic apart from building 'SavedEvent's.
          let saved = (\(num, evt) -> SavedEvent num evt Nothing) <$> zip [0..] events
          notifySubs self name saved
          let Just lastOne = getLast $ foldMap (Last . Just) saved
              nextNum = eventNumber lastOne + 1
          return nextNum


        Just stream -> do
          let currentNumber = streamNextNumber stream
          case ver of
            NoStream ->
              STM.throwSTM $ ExpectedVersionException ver StreamExists
            ExactVersion v ->
              unless (v == streamNextNumber stream - 1)
                $ STM.throwSTM
                $ ExpectedVersionException ver (ExactVersion currentNumber)

            _ -> return ()

          let nextStream = appendStream events stream
              newStreamMap = Map.adjust (const nextStream) name streamMap

          STM.writeTVar _streams newStreamMap

          -- This part is already performed in 'appendStream' but difficult
          -- to take its logic apart from building 'SavedEvent's.
          let saved = (\(num, evt) -> SavedEvent num evt Nothing) <$> zip [currentNumber..] events
          notifySubs self name saved
          let Just lastOne = getLast $ foldMap (Last . Just) saved
              nextNum = eventNumber lastOne + 1
          return nextNum


  readBatch StubStore{..} name (Batch start _) = liftBase $ async $ STM.atomically $ do
    streamMap <- STM.readTVar _streams
    case Map.lookup name streamMap of
      Nothing -> return $ ReadFailure $ StreamNotFound name
      Just stream -> do
        let events = S.filter ((>= start) . eventNumber) $ streamEvents stream
            slice = Slice { sliceEvents = toList events
                          , sliceEndOfStream = True
                          , sliceNextEventNumber = streamNextNumber stream
                          }

        return $ ReadSuccess slice

  subscribe StubStore{..} name = do
    sid <- freshSubscriptionId
    liftBase $ STM.atomically $ do
      chan <- STM.newTChan
      let sub = Subscription sid $ liftBase $ STM.atomically $ do
            saved <- STM.readTChan chan
            return $ Right saved

      subMap <- STM.readTVar _subs
      let _F Nothing  = Just $ Map.singleton sid  chan
          _F (Just m) = Just $ Map.insert sid chan m

          nextSubMap = Map.alter _F name subMap

      STM.writeTVar _subs nextSubMap
      return sub
