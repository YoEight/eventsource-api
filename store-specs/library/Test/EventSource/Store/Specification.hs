{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
--------------------------------------------------------------------------------
-- |
-- Module : Test.EventSource.Store.Specification
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module Test.EventSource.Store.Specification (specification) where

--------------------------------------------------------------------------------
import ClassyPrelude
import Control.Monad.Except
import Control.Monad.State.Strict
import Data.Aeson.Types
import Data.UUID
import Data.UUID.V4
import EventSource
import Test.Tasty.Hspec

--------------------------------------------------------------------------------
newtype TestEvent = TestEvent Int deriving (Eq, Show)

--------------------------------------------------------------------------------
instance EncodeEvent TestEvent where
  encodeEvent (TestEvent v) = do
    setEventType "test-event"
    setEventPayload $ dataFromJson $ object [ "value" .= v ]

--------------------------------------------------------------------------------
instance DecodeEvent TestEvent where
  decodeEvent Event{..} = do
    unless (eventType == "test-event") $
      Left "Wrong event type"

    dataAsParse eventPayload $ withObject "" $ \o ->
      fmap TestEvent (o .: "value")

--------------------------------------------------------------------------------
freshStreamName :: MonadIO m => m StreamName
freshStreamName = liftIO $ fmap (StreamName . toText) nextRandom

--------------------------------------------------------------------------------
incr :: Int -> Int
incr = (+1)

--------------------------------------------------------------------------------
specification :: Store store => store -> Spec
specification store = do
  specify "API - Add event" $ do
    let expected = TestEvent 1
    name <- freshStreamName
    _ <- waitAsync =<< appendEvent store name AnyVersion expected
    res <- waitAsync =<< readBatch store name (startFrom 0)

    res `shouldSatisfy` isReadSuccess
    let ReadSuccess slice = res

    for_ (zip [0..] $ sliceEvents slice) $ \(num, e) ->
      eventNumber e `shouldBe` num

    sliceEventsAs slice `shouldBe` Right [expected]

  specify "API - Read events in batch" $ do
    let expected = fmap TestEvent [1..3]
    name <- freshStreamName
    _ <- waitAsync =<< appendEvents store name AnyVersion
                         expected
    res <- streamIterator store name

    res `shouldSatisfy` isReadSuccess

    let ReadSuccess i = res
    got <- iteratorReadAllEvents i

    got `shouldBe` expected

  specify "API - Subscription working" $ do
    let expected = TestEvent 1
    name <- freshStreamName
    sub <- subscribe store name

    _ <- waitAsync =<< appendEvent store name AnyVersion expected

    res <- nextEventAs sub
    res `shouldSatisfy` either (const False) (const True)

    let Right got = res
    got `shouldBe` expected

  specify "API - forEvents" $ do
    let events = fmap TestEvent [0..9]
    name <- freshStreamName
    _ <- waitAsync =<< appendEvents store name AnyVersion events

    let action = do
          forEvents store name $ \(_ :: TestEvent) ->
            modify incr
          get

    res <- runExceptT $ mapExceptT (\m -> evalStateT m 0) action

    res `shouldSatisfy` either (const False) (const True)
    let Right st = res

    st `shouldBe` (10 :: Int)

  specify "API - foldEvents" $ do
    let events = fmap TestEvent [0..9]
    name <- freshStreamName
    _ <- waitAsync =<< appendEvents store name AnyVersion events

    res <- runExceptT $ foldEvents store name
                      (\s (_ :: TestEvent) -> s + 1)
                      0

    res `shouldSatisfy` either (const False) (const True)
    let Right st = res

    st `shouldBe` (10 :: Int)

  specify "API - Iterator.readAllEvents" $ do
    let events = fmap TestEvent [0..9]
    name <- freshStreamName
    _ <- waitAsync =<< appendEvents store name AnyVersion events

    res <- streamIterator store name
    res `shouldSatisfy` isReadSuccess
    let ReadSuccess i = res

    got <- iteratorReadAllEvents i
    got `shouldBe` events
