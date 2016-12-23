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
import EventSource
import EventSource.Store.Stub
import Test.Tasty (TestTree)
import Test.Tasty.Hspec

--------------------------------------------------------------------------------
import           Test.EventSource.Event
import qualified Test.EventSource.Store.Api as Api

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
    res <- readBatch stub "test-1" (startFrom 0)

    res `shouldSatisfy` isReadSuccess
    let ReadSuccess slice = res

    for_ (zip [0..] $ sliceEvents slice) $ \(num, e) ->
      eventNumber e `shouldBe` num

    sliceEventsAs slice `shouldBe` Right [expected]

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

  Api.spec stub
