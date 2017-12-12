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
import           Control.Concurrent.STM (STM)
import qualified Control.Concurrent.STM as STM
import Data.Foldable (toList, for_, foldl')

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
  Stream { streamNumber :: EventNumber
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
data Version
  = NoStreamYet
  | Version EventNumber

--------------------------------------------------------------------------------
getCurrentVersion :: StreamName -> Map StreamName Stream -> Version
getCurrentVersion name m =
  case Map.lookup name m of
    Nothing -> NoStreamYet
    Just s  -> Version $ streamNumber s

--------------------------------------------------------------------------------
data Registered =
  Registered { _regSavedEvents :: !(Seq SavedEvent)
             , _regNumber      :: !EventNumber
             , _regNewMap      :: !(Map StreamName Stream)
             }

--------------------------------------------------------------------------------
registerEvents :: StreamName
               -> [Event]
               -> Map StreamName Stream
               -> Registered
registerEvents name xs m =
  case Map.lookup name m of
    Nothing     -> appendStream (Stream (-1) mempty)
    Just stream -> appendStream stream
  where
    appendStream stream =
      let
          cur       = streamNumber stream
          nextNum   = cur + fromIntegral (length xs)
          saved     = appEvents cur (streamEvents stream)
          newStream = Stream nextNum saved
          regd      = Registered saved nextNum (Map.insert name newStream m)

       in regd

    appEvents ver xss =
      let go (acc, cur) evt =
            let next   = cur + 1
                saved  = SavedEvent next evt Nothing
                newAcc = acc |> saved

             in (newAcc, next)

       in fst $ foldl' go (xss,ver) xs

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
notifySubs :: Foldable f => StubStore -> StreamName -> f SavedEvent -> STM ()
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

      let persistEvents =
            do let regd = registerEvents name events streamMap

               notifySubs self name $ _regSavedEvents regd
               (_regNumber regd) <$ STM.writeTVar _streams (_regNewMap regd)

      case getCurrentVersion name streamMap of
        NoStreamYet ->
          case ver of
            NoStream   -> persistEvents
            AnyVersion -> persistEvents
            _          -> STM.throwSTM $ ExpectedVersionException ver NoStream
        Version num ->
          case ver of
            AnyVersion   -> persistEvents
            StreamExists -> persistEvents
            NoStream     -> STM.throwSTM $ ExpectedVersionException ver
                                         $ ExactVersion num
            ExactVersion expVer
              | expVer == num -> persistEvents
              | otherwise     -> STM.throwSTM $ ExpectedVersionException ver
                                              $ ExactVersion num

  readBatch StubStore{..} name (Batch start _) = liftBase $ async $ STM.atomically $ do
    streamMap <- STM.readTVar _streams
    case Map.lookup name streamMap of
      Nothing -> return $ ReadFailure $ StreamNotFound name
      Just stream -> do
        let events = S.filter ((>= start) . eventNumber) $ streamEvents stream
            slice = Slice { sliceEvents = toList events
                          , sliceEndOfStream = True
                          , sliceNextEventNumber = streamNumber stream
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
