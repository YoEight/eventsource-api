{-# LANGUAGE FlexibleContexts    #-}
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
import Control.Monad (unless)
import Data.Foldable (for_)

--------------------------------------------------------------------------------
import Control.Concurrent.Async (wait)
import Control.Monad.Except (runExceptT, mapExceptT)
import Control.Monad.Base (MonadBase, liftBase)
import Control.Monad.State (evalStateT, get, modify)
import Data.Aeson.Types (object, withObject, (.=), (.:))
import Data.UUID (toText)
import Data.UUID.V4 (nextRandom)
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
freshStreamName :: MonadBase IO m => m StreamName
freshStreamName = liftBase $ fmap (StreamName . toText) nextRandom

--------------------------------------------------------------------------------
incr :: Int -> Int
incr = (+1)

--------------------------------------------------------------------------------
specification :: Store store => store -> Spec
specification store = do
  specify "API - Add event" $ do
    let expected = TestEvent 1
    name <- freshStreamName
    _ <- wait =<< appendEvent store name AnyVersion expected
    res <- wait =<< readBatch store name (startFrom 0)

    res `shouldSatisfy` isReadSuccess
    let ReadSuccess slice = res

    for_ (zip [0..] $ sliceEvents slice) $ \(num, e) ->
      eventNumber e `shouldBe` num

    sliceEventsAs slice `shouldBe` Right [expected]

  specify "API - Read events in batch" $ do
    let expected = fmap TestEvent [1..3]
    name <- freshStreamName
    _ <- wait =<< appendEvents store name AnyVersion
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

    _ <- wait =<< appendEvent store name AnyVersion expected

    res <- nextEventAs sub
    res `shouldSatisfy` either (const False) (const True)

    let Right got = res
    got `shouldBe` expected

  specify "API - forEvents" $ do
    let events = fmap TestEvent [0..9]
    name <- freshStreamName
    _ <- wait =<< appendEvents store name AnyVersion events

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
    _ <- wait =<< appendEvents store name AnyVersion events

    res <- runExceptT $ foldEvents store name
                      (\s (_ :: TestEvent) -> s + 1)
                      0

    res `shouldSatisfy` either (const False) (const True)
    let Right st = res

    st `shouldBe` (10 :: Int)

  specify "API - forSavedEvents" $ do
    let events = fmap TestEvent [0..9]
    name <- freshStreamName
    _ <- wait =<< appendEvents store name AnyVersion events

    let action = do
          forSavedEvents store name $ \(_ :: SavedEvent) -> modify incr
          get

    res <- runExceptT $ mapExceptT (\m -> evalStateT m 0) action

    res `shouldSatisfy` either (const False) (const True)
    let Right st = res

    st `shouldBe` (10 :: Int)

  specify "API - foldSavedEvents" $ do
    let values = [0..9]
        events = fmap TestEvent values
        seed   = Right (0, EventNumber 0)

        testFold (Left e) _           = Left e
        testFold (Right (a, n)) saved =
          let n' = eventNumber saved
              ee = decodeEvent $ savedEvent saved in
          case ee of
            Left t               -> Left t
            Right (TestEvent a') -> Right (a + a', max n n')

    name <- freshStreamName
    _ <- wait =<< appendEvents store name AnyVersion events

    res <- runExceptT $ foldSavedEvents store name testFold seed

    res `shouldSatisfy` either (const False) (const True)
    let Right st = res

    st `shouldBe` Right (sum values, EventNumber 9)

  specify "API - Iterator.readAllEvents" $ do
    let events = fmap TestEvent [0..9]
    name <- freshStreamName
    _ <- wait =<< appendEvents store name AnyVersion events

    res <- streamIterator store name
    res `shouldSatisfy` isReadSuccess
    let ReadSuccess i = res

    got <- iteratorReadAllEvents i
    got `shouldBe` events
