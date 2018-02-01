{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
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
import Control.Exception (Exception)
import Control.Monad (MonadPlus, mzero)
import Data.Bifunctor (first)
import Data.Foldable (foldlM)
import Data.Int (Int64)
import Data.String (IsString(..))
import Data.String.Conversions (convertString)

--------------------------------------------------------------------------------
import           Control.Monad.Base (MonadBase, liftBase)
import           Control.Monad.State.Strict (State, modify, put)
import           Data.Aeson (ToJSON(..), FromJSON(..), Value, (.=), (.:))
import qualified Data.Aeson as Aeson
import           Data.Aeson.Types (Parser, parseEither)
import           Data.ByteString (ByteString)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Map.Strict as Map
import           Data.Text (Text)
import           Data.UUID (UUID, toText, fromText)
import           Data.UUID.V4 (nextRandom)

--------------------------------------------------------------------------------
-- | Opaque data type used to store raw data.
data Data
  = Data ByteString
  | DataAsJson Value

--------------------------------------------------------------------------------
instance Show Data where
  show _ = "Data(*Binary data*)"

--------------------------------------------------------------------------------
-- | Sometimes, having to implement a 'FromJSON' instance isn't flexible enough.
--   'JsonParsing' allow to pass parameters when parsing from a JSON value while
--   remaining composable.
newtype JsonParsing a = JsonParsing (Value -> Parser a)

--------------------------------------------------------------------------------
instance Functor JsonParsing where
  fmap f (JsonParsing k) = JsonParsing $ \v -> f <$> k v

--------------------------------------------------------------------------------
instance Applicative JsonParsing where
  pure a = JsonParsing $ \_ -> return a

  (JsonParsing kf) <*> (JsonParsing ka) =
    JsonParsing $ \v ->
      kf v <*> ka v

--------------------------------------------------------------------------------
instance Monad JsonParsing where
  return = pure

  JsonParsing k >>= f = JsonParsing $ \v -> do
    a <- k v
    let JsonParsing km = f a
    km v

--------------------------------------------------------------------------------
-- | Returns 'Data' content as a 'ByteString'.
dataAsBytes :: Data -> ByteString
dataAsBytes (Data bs) = bs
dataAsBytes (DataAsJson v) = convertString $ Aeson.encode v

--------------------------------------------------------------------------------
-- | Creates a 'Data' object from a raw 'ByteString'.
dataFromBytes :: ByteString -> Data
dataFromBytes = Data

--------------------------------------------------------------------------------
-- | Creates a 'Data' object from a JSON object.
dataFromJson :: ToJSON a => a -> Data
dataFromJson = DataAsJson . toJSON

--------------------------------------------------------------------------------
-- | Returns 'Data' content as any value that implements 'FromJSON' type-class.
dataAsJson :: FromJSON a => Data -> Either Text a
dataAsJson (Data bs) = first convertString $ Aeson.eitherDecodeStrict bs
dataAsJson (DataAsJson v) = first convertString $ parseEither parseJSON v

--------------------------------------------------------------------------------
-- | Uses a 'JsonParsing' comuputation to extract a value.
dataAsParsing :: Data -> JsonParsing a -> Either Text a
dataAsParsing dat (JsonParsing k) = do
  value <- dataAsJson dat
  first convertString $ parseEither k value

--------------------------------------------------------------------------------
-- | Like 'dataAsParsing' but doesn't require you to use 'JsonParsing'.
dataAsParse :: Data -> (Value -> Parser a) -> Either Text a
dataAsParse dat k = dataAsParsing dat $ JsonParsing k

--------------------------------------------------------------------------------
-- | Used to store a set a properties. One example is to be used as 'Event'
--   metadata.
newtype Properties = Properties (Map.Map Text Text)

--------------------------------------------------------------------------------
instance Monoid Properties where
  mempty = Properties mempty
  mappend (Properties a) (Properties b) = Properties $ mappend a b

--------------------------------------------------------------------------------
instance Show Properties where
  show (Properties m) = show m

--------------------------------------------------------------------------------
instance ToJSON Properties where
  toJSON = Aeson.object . fmap go . properties
    where
      go (k, v) = k .= v

--------------------------------------------------------------------------------
instance FromJSON Properties where
  parseJSON = Aeson.withObject "Properties" $ \o ->
    let go p k = fmap (\v -> setProperty k v p) (o .: k) in
    foldlM go mempty (HashMap.keys o)

--------------------------------------------------------------------------------
-- | Retrieves a value associated with the given key.
property :: MonadPlus m => Text -> Properties -> m Text
property k (Properties m) =
  case Map.lookup k m of
    Nothing -> mzero
    Just v -> return v

--------------------------------------------------------------------------------
-- | Builds a 'Properties' with a single pair of key-value.
singleton :: Text -> Text -> Properties
singleton k v = setProperty k v mempty

--------------------------------------------------------------------------------
-- | Adds a pair of key-value into given 'Properties'.
setProperty :: Text -> Text -> Properties -> Properties
setProperty key value (Properties m) = Properties $ Map.insert key value m

--------------------------------------------------------------------------------
-- | Returns all associated key-value pairs as a list.
properties :: Properties -> [(Text, Text)]
properties (Properties m) = Map.toList m

--------------------------------------------------------------------------------
-- | Used to identify an event.
newtype EventId = EventId UUID deriving (Eq, Ord)

--------------------------------------------------------------------------------
instance ToJSON EventId where
  toJSON (EventId u) = toJSON (toText u)

--------------------------------------------------------------------------------
instance FromJSON EventId where
  parseJSON = Aeson.withText "EventId" $ \t ->
    case fromText t of
      Just u  -> return $ EventId u
      Nothing -> mzero

--------------------------------------------------------------------------------
instance Show EventId where
  show (EventId uuid) = show uuid

--------------------------------------------------------------------------------
-- | Generates a fresh 'EventId'.
freshEventId :: MonadBase IO m => m EventId
freshEventId = fmap EventId $ liftBase nextRandom

--------------------------------------------------------------------------------
-- | Represents a stream name.
newtype StreamName = StreamName Text deriving (Eq, Ord, ToJSON, FromJSON)

--------------------------------------------------------------------------------
instance Show StreamName where
  show (StreamName s) = show s

--------------------------------------------------------------------------------
instance IsString StreamName where
  fromString = StreamName . fromString

--------------------------------------------------------------------------------
-- | Used to identity the type of an 'Event'.
newtype EventType = EventType Text deriving (Eq, ToJSON, FromJSON)

--------------------------------------------------------------------------------
instance Show EventType where
  show (EventType t) = show t

--------------------------------------------------------------------------------
instance IsString EventType where
  fromString = EventType . fromString

--------------------------------------------------------------------------------
-- | Sets 'EventType' for an 'Event'.
setEventType :: EventType -> State Event ()
setEventType typ = modify $ \s -> s { eventType = typ }

--------------------------------------------------------------------------------
-- | Sets 'Eventid' for an 'Event'.
setEventId :: EventId -> State Event ()
setEventId eid = modify $ \s -> s { eventId = eid }

--------------------------------------------------------------------------------
-- | Sets a payload for an 'Event'.
setEventPayload :: Data -> State Event ()
setEventPayload dat = modify $ \s -> s { eventPayload = dat }

--------------------------------------------------------------------------------
-- | Sets metadata for an 'Event'.
setEventMetadata :: Properties -> State Event ()
setEventMetadata props = modify $ \s -> s { eventMetadata = Just props }

--------------------------------------------------------------------------------
-- | Encapsulates an event which is about to be saved.
data Event =
  Event { eventType :: EventType
        , eventId :: EventId
        , eventPayload :: Data
        , eventMetadata :: Maybe Properties
        } deriving Show

--------------------------------------------------------------------------------
-- | Represents an event index in a stream.
newtype EventNumber = EventNumber Int64
  deriving (Eq, Ord, Num, Enum, Show, FromJSON, ToJSON)

--------------------------------------------------------------------------------
-- | Represents an event that's saved into the event store.
data SavedEvent =
  SavedEvent { eventNumber :: EventNumber
             , savedEvent :: Event
             , linkEvent :: Maybe Event
             } deriving Show

--------------------------------------------------------------------------------
-- | Deserializes a 'SavedEvent'.
savedEventAs :: DecodeEvent a => SavedEvent -> Either Text a
savedEventAs = decodeEvent . savedEvent

--------------------------------------------------------------------------------
-- | Represents batch of events read from a store.
data Slice =
  Slice { sliceEvents :: [SavedEvent]
        , sliceEndOfStream :: Bool
        , sliceNextEventNumber :: EventNumber
        } deriving Show

--------------------------------------------------------------------------------
-- | Deserializes a 'Slice''s events.
sliceEventsAs :: DecodeEvent a => Slice -> Either Text [a]
sliceEventsAs = traverse savedEventAs . sliceEvents

--------------------------------------------------------------------------------
-- | Encodes a data object into an 'Event'. 'encodeEvent' get passed an
--   'EventId' in a case where a fresh id is needed.
class EncodeEvent a where
  encodeEvent :: a -> State Event ()

--------------------------------------------------------------------------------
-- | Decodes an 'Event' into a data object.
class DecodeEvent a where
  decodeEvent :: Event -> Either Text a

--------------------------------------------------------------------------------
newtype DecodeEventException = DecodeEventException Text deriving Show

--------------------------------------------------------------------------------
instance Exception DecodeEventException

--------------------------------------------------------------------------------
instance EncodeEvent Event where
  encodeEvent e = put e

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
  | ExactVersion EventNumber
    -- Stream should be at givent event number.
  deriving (Show, Eq)

--------------------------------------------------------------------------------
-- | Statuses you can get on every read attempt.
data ReadStatus a
  = ReadSuccess a
  | ReadFailure ReadFailure
  deriving Show

--------------------------------------------------------------------------------
-- | Returns 'True' is 'ReadStatus' is a 'ReadSuccess'.
isReadSuccess :: ReadStatus a -> Bool
isReadSuccess (ReadSuccess _) = True
isReadSuccess _ = False

--------------------------------------------------------------------------------
-- | Returns 'False' is 'ReadStatus' is a 'ReadFailure'.
isReadFailure :: ReadStatus a -> Bool
isReadFailure (ReadFailure _) = True
isReadFailure _ = False

--------------------------------------------------------------------------------
-- | Represents the different kind of failure you can get when reading.
data ReadFailure
  = StreamNotFound StreamName
  | ReadError (Maybe Text)
  | AccessDenied StreamName
  deriving Show

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
