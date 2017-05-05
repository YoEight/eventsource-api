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
import           Protolude
import           Data.Aeson
import           Data.Aeson.Types
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
            Data bs -> GES.withBinaryAndMetadata bs (toS $ encode p)
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
fromGesReadResult :: GES.ReadResult t a -> ReadStatus a
fromGesReadResult (GES.ReadSuccess a) =
  ReadSuccess a
fromGesReadResult GES.ReadNoStream =
  ReadFailure StreamNotFound
fromGesReadResult (GES.ReadStreamDeleted _) =
  ReadFailure StreamNotFound
fromGesReadResult GES.ReadNotModified =
  ReadFailure (ReadError $ Just "not modified")
fromGesReadResult (GES.ReadError e) =
  ReadFailure $ ReadError e
fromGesReadResult (GES.ReadAccessDenied _) =
  ReadFailure AccessDenied

--------------------------------------------------------------------------------
defaultBatchSize :: Int32
defaultBatchSize = 500

--------------------------------------------------------------------------------
instance Store GetEventStore where
  appendEvents (GetEventStore conn) (StreamName name) ver xs = liftIO $ do
    events <- traverse makeEvent xs
    w <- GES.sendEvents conn name (toGesExpVer ver) events
    return $ fmap (EventNumber . GES.writeNextExpectedVersion) w

  readBatch (GetEventStore conn) (StreamName name) b = liftIO $ do
    let EventNumber n = batchFrom b
    w <- GES.readStreamEventsForward conn name n (batchSize b) True
    return $ fmap (fmap fromGesSlice . fromGesReadResult) w

  subscribe (GetEventStore conn) (StreamName name) = liftIO $ do
    sub <- GES.subscribe conn name True
    sid <- freshSubscriptionId

    return $ Subscription sid $ liftIO $
      try $ fmap fromGesEvent $ GES.nextEvent sub

--------------------------------------------------------------------------------
-- | Returns a GetEventStore based store implementation.
gesStore :: GES.Settings -> GES.ConnectionType -> IO GetEventStore
gesStore setts typ = fmap GetEventStore $ GES.connect setts typ
