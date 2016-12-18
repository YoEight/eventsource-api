--------------------------------------------------------------------------------
-- |
-- Module : EventSource.Types
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module EventSource.Types where

--------------------------------------------------------------------------------
import ClassyPrelude
import Data.Aeson
import Data.UUID hiding (fromString)
import Data.UUID.V4

--------------------------------------------------------------------------------
newtype Data = Data ByteString

--------------------------------------------------------------------------------
instance Show Data where
  show _ = "Data(*Binary data*)"

--------------------------------------------------------------------------------
-- | Returns 'Data' content as a 'ByteString'.
dataAsBytes :: Data -> ByteString
dataAsBytes (Data bs) = bs

--------------------------------------------------------------------------------
-- | Creates a 'Data' object from a raw 'ByteString'.
dataFromBytes :: ByteString -> Data
dataFromBytes = Data

--------------------------------------------------------------------------------
-- | Creates a 'Data' object from a JSON object.
dataFromJson :: ToJSON a => a -> Data
dataFromJson = dataFromBytes . toStrict . encode

--------------------------------------------------------------------------------
-- | Returns 'Data' content as any value that implements 'FromJSON' type-class.
dataAsJson :: FromJSON a => Data -> Either String a
dataAsJson = eitherDecodeStrict . dataAsBytes

--------------------------------------------------------------------------------
-- | Used to identify an event.
newtype EventId = EventId UUID deriving (Eq, Ord)

--------------------------------------------------------------------------------
instance Show EventId where
  show (EventId uuid) = show uuid

--------------------------------------------------------------------------------
-- | Generates a fresh 'EventId'.
freshEventId :: IO EventId
freshEventId = fmap EventId nextRandom

--------------------------------------------------------------------------------
-- | Used to identity the type of an 'Event'.
newtype EventType = EventType Text deriving Eq

--------------------------------------------------------------------------------
instance Show EventType where
  show (EventType t) = show t

--------------------------------------------------------------------------------
instance IsString EventType where
  fromString = EventType . fromString

--------------------------------------------------------------------------------
-- | Encapsulates an event which is about to be saved.
data Event =
  Event { eventType :: EventType
        , eventId :: EventId
        , eventPayload :: Data
        , eventMetadata :: Data
        }

--------------------------------------------------------------------------------
-- | Encodes a data object into an 'Event'.
class EncodeEvent a where
  encodeEvent :: a -> EventId -> Event

--------------------------------------------------------------------------------
-- | Decodes an 'Event' into a data object.
class DecodeEvent a where
  decodeEvent :: Event -> Either Text a

--------------------------------------------------------------------------------
-- | The purpose of 'ExpectedVersion' is to make sure a certain stream state is
--   at an expected point in order to carry out a write.
data ExpectedVersion
  = AnyVersion
    -- Stream is a any given state.
  | NoStream
    -- Stream shouldn't exist.
  | StreamExists
    -- Stream should exist.
  | ExactVersion Int32
    -- Stream should be at givent event number.
