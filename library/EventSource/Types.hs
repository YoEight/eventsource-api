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
import Data.Foldable

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
-- | Used to store a set a properties. One example is to be used as 'Event'
--   metadata.
newtype Properties = Properties (Map Text Text)

--------------------------------------------------------------------------------
instance Monoid Properties where
  mempty = Properties mempty
  mappend (Properties a) (Properties b) = Properties $ mappend a b

--------------------------------------------------------------------------------
instance Show Properties where
  show (Properties m) = show m

--------------------------------------------------------------------------------
-- | Retrieves a value associated with the given key.
property :: MonadPlus m => Text -> Properties -> m Text
property k (Properties m) =
  case lookup k m of
    Nothing -> mzero
    Just v -> return v

--------------------------------------------------------------------------------
-- | Builds a 'Properties' with a single pair of key-value.
singleton :: Text -> Text -> Properties
singleton k v = setProperty k v mempty

--------------------------------------------------------------------------------
-- | Adds a pair of key-value into given 'Properties'.
setProperty :: Text -> Text -> Properties -> Properties
setProperty key value (Properties m) = Properties $ insertMap key value m

--------------------------------------------------------------------------------
-- | Returns all associated key-value pairs as a list.
properties :: Properties -> [(Text, Text)]
properties (Properties m) = mapToList m

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
-- | Represents a stream name.
newtype StreamName = StreamName Text deriving Eq

--------------------------------------------------------------------------------
instance Show StreamName where
  show (StreamName s) = show s

--------------------------------------------------------------------------------
instance IsString StreamName where
  fromString = StreamName . fromString

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
        , eventMetadata :: Maybe Properties
        }

--------------------------------------------------------------------------------
-- | Represents an event that's saved into the event store.
data SavedEvent =
  SavedEvent { eventNumber :: Int32
             , savedEvent :: Event
             }

--------------------------------------------------------------------------------
data Slice =
  Slice { sliceEvents :: [SavedEvent]
        , sliceEndOfStream :: Bool
        , sliceNextEventNumber :: Int32
        }

--------------------------------------------------------------------------------
-- | Encodes a data object into an 'Event'. 'encodeEvent' get passed an
--   'EventId' in a case where a fresh id is needed.
class EncodeEvent a where
  encodeEvent :: a -> EventId -> Event

--------------------------------------------------------------------------------
instance EncodeEvent Event where
  encodeEvent = const

--------------------------------------------------------------------------------
-- | Decodes an 'Event' into a data object.
class DecodeEvent a where
  decodeEvent :: Event -> Either Text a

--------------------------------------------------------------------------------
instance DecodeEvent Event where
  decodeEvent = Right

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

--------------------------------------------------------------------------------
-- | Statuses you can get on every read attempt.
data ReadStatus a
  = ReadSuccess a
  | ReadFailure ReadFailure

--------------------------------------------------------------------------------
-- | Represents the different kind of failure you can get when reading.
data ReadFailure
  = StreamNotFound
  | ReadError (Maybe Text)
  | AccessDenied Text

--------------------------------------------------------------------------------
instance Functor ReadStatus where
  fmap f (ReadSuccess a)  = ReadSuccess $ f a
  fmap _ (ReadFailure e)  = ReadFailure e

--------------------------------------------------------------------------------
instance Foldable ReadStatus where
  foldMap f (ReadSuccess a) = f a
  foldMap _ _               = mempty

--------------------------------------------------------------------------------
instance Traversable ReadStatus where
  traverse f (ReadSuccess a) = fmap ReadSuccess $ f a
  traverse _ (ReadFailure e) = pure $ ReadFailure e
