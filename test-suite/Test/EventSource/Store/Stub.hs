{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
--------------------------------------------------------------------------------
-- |
-- Module : Test.EventSource.Store.Stub
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module Test.EventSource.Store.Stub (test) where

--------------------------------------------------------------------------------
import ClassyPrelude
import Data.Aeson
import EventSource
import EventSource.Store.Stub
import Test.Tasty (TestTree)
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
test :: IO TestTree
test = testSpec "Store Stub" spec

--------------------------------------------------------------------------------
spec :: Spec
spec = parallel $ do
  stub <- runIO newStub

  it "should add event" $ do
    let expected = TestEvent 1
    appendEvent stub "test-1" AnyVersion expected
    eventOpt <- lastStreamEvent stub "test-1"

    eventOpt `shouldSatisfy` isJust
    let Just saved = eventOpt

    eventNumber saved `shouldBe` 0

    let deserialized = decodeEvent $ savedEvent saved

    deserialized `shouldBe` Right expected

  it "should read events batch" $ do
    let expected = fmap TestEvent [1..3]
    appendEvents stub "test-2" AnyVersion expected
    res <- streamIterator stub "test-2"

    res `shouldSatisfy` isReadSuccess

    let ReadSuccess i = res
    got <- iteratorReadAllEvents i

    got `shouldBe` expected

  it "should allow subscription" $ do
    let expected = TestEvent 1
    sub <- subscribe stub "test-3"

    appendEvent stub "test-3" AnyVersion expected

    res <- nextEventAs sub
    res `shouldSatisfy` either (const False) (const True)

    let Right got = res
    got `shouldBe` expected
