{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
--------------------------------------------------------------------------------
-- |
-- Module : EventSource.Store.GetEventStore
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
-- This module exposes a GetEventStore implementation of Store interface.
--------------------------------------------------------------------------------
module EventSource.Store.GetEventStore
  ( GetEventStore
  , gesConnection
  , gesStore
  ) where

--------------------------------------------------------------------------------
import Control.Exception (try)

--------------------------------------------------------------------------------
import           Control.Monad.Base (MonadBase, liftBase)
import           Control.Monad.State.Strict (execState)
import           Data.Aeson (encode, decodeStrict)
import           Data.String.Conversions (convertString)
import qualified Database.EventStore as GES
import           EventSource

--------------------------------------------------------------------------------
newtype GetEventStore = GetEventStore { gesConnection :: GES.Connection }

--------------------------------------------------------------------------------
toGesExpVer :: ExpectedVersion -> GES.ExpectedVersion
toGesExpVer AnyVersion = GES.anyVersion
toGesExpVer NoStream = GES.noStreamVersion
toGesExpVer StreamExists = GES.streamExists
toGesExpVer (ExactVersion n) =
  let EventNumber i = n in
  GES.exactEventVersion i

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
makeEvent :: EncodeEvent a => a -> IO GES.Event
makeEvent a = toGesEvent <$> buildEvent a

--------------------------------------------------------------------------------
toGesEvent :: Event -> GES.Event
toGesEvent e = GES.createEvent (GES.UserDefined typ) (Just eid) eventData
  where
    EventType typ = eventType e
    EventId eid = eventId e

    eventData =
      case eventMetadata e of
        Nothing ->
          case eventPayload e of
            Data bs -> GES.withBinary bs
            DataAsJson v -> GES.withJson v
        Just p ->
          case eventPayload e of
            Data bs -> GES.withBinaryAndMetadata bs (convertString $ encode p)
            DataAsJson v -> GES.withJsonAndMetadata v p

--------------------------------------------------------------------------------
fromGesEvent :: GES.ResolvedEvent -> SavedEvent
fromGesEvent e@(GES.ResolvedEvent recEvt lnkEvt _) = saved
  where
    re =
      case recEvt of
        Just result -> result
        _           -> GES.resolvedEventOriginal e

    make evt =
      let eid = EventId $ GES.recordedEventId evt
          etyp = EventType $ GES.recordedEventType evt
          payload = GES.recordedEventData evt
          metaBytes = GES.recordedEventMetadata evt in
      Event { eventType = etyp
            , eventId = eid
            , eventPayload = dataFromBytes payload
            , eventMetadata = decodeStrict =<< metaBytes
            }

    saved = SavedEvent { eventNumber = EventNumber $ GES.recordedEventNumber re
                       , savedEvent = make re
                       , linkEvent = fmap make lnkEvt
                       }

--------------------------------------------------------------------------------
fromGesSlice :: GES.StreamSlice -> Slice
fromGesSlice s = Slice { sliceEvents = fromGesEvent <$> GES.sliceEvents s
                       , sliceEndOfStream = GES.sliceEOS s
                       , sliceNextEventNumber = EventNumber $ GES.sliceNext s
                       }

--------------------------------------------------------------------------------
fromGesReadResult :: StreamName -> GES.ReadResult t a -> ReadStatus a
fromGesReadResult _ (GES.ReadSuccess a) =
  ReadSuccess a
fromGesReadResult n GES.ReadNoStream =
  ReadFailure $ StreamNotFound n
fromGesReadResult n (GES.ReadStreamDeleted _) =
  ReadFailure $ StreamNotFound n
fromGesReadResult _ GES.ReadNotModified =
  ReadFailure (ReadError $ Just "not modified")
fromGesReadResult _ (GES.ReadError e) =
  ReadFailure $ ReadError e
fromGesReadResult n (GES.ReadAccessDenied _) =
  ReadFailure $ AccessDenied n

--------------------------------------------------------------------------------
toGESStreamName :: StreamName -> GES.StreamName
toGESStreamName (StreamName name) = GES.StreamName name

--------------------------------------------------------------------------------
instance Store GetEventStore where
  appendEvents (GetEventStore conn) name ver xs = liftBase $ do
    events <- traverse makeEvent xs
    w <- GES.sendEvents conn (toGESStreamName name) (toGesExpVer ver) events
          Nothing
    return $ fmap (EventNumber . GES.writeNextExpectedVersion) w

  readBatch (GetEventStore conn) name b = liftBase $ do
    let EventNumber n = batchFrom b
    w <- GES.readStreamEventsForward conn (toGESStreamName name) n (batchSize b)
          True Nothing
    return $ fmap (fmap fromGesSlice . fromGesReadResult name) w

  subscribe (GetEventStore conn) name = liftBase $ do
    sub <- GES.subscribe conn (toGESStreamName name) True Nothing
    sid <- freshSubscriptionId

    return $ Subscription sid $ liftBase $
      try $ fmap fromGesEvent $ GES.nextEvent sub

--------------------------------------------------------------------------------
-- | Returns a GetEventStore based store implementation.
gesStore :: GES.Settings -> GES.ConnectionType -> IO GetEventStore
gesStore setts typ = fmap GetEventStore $ GES.connect setts typ
