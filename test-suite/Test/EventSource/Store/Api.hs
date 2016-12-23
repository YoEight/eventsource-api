{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
--------------------------------------------------------------------------------
-- |
-- Module : Test.EventSource.Store.Api
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module Test.EventSource.Store.Api (spec) where

--------------------------------------------------------------------------------
import ClassyPrelude
import Control.Monad.Except
import Control.Monad.State.Strict
import EventSource
import Test.Tasty.Hspec

--------------------------------------------------------------------------------
import Test.EventSource.Event

--------------------------------------------------------------------------------
incr :: Int -> Int
incr = (+1)

--------------------------------------------------------------------------------
spec :: Store store => store -> Spec
spec store = do
  specify "API - forEvents" $ do
    let events = fmap TestEvent [0..9]

    appendEvents store "for-events" AnyVersion events

    let action = do
          forEvents store "for-events" $ \(_ :: TestEvent) ->
            modify incr
          get

    res <- runExceptT $ mapExceptT (\m -> evalStateT m 0) action

    res `shouldSatisfy` either (const False) (const True)
    let Right st = res

    st `shouldBe` (10 :: Int)

  specify "API - foldEvents" $ do
    let events = fmap TestEvent [0..9]

    appendEvents store "fold-events" AnyVersion events

    res <- runExceptT $ foldEvents store "fold-events"
                      (\s (_ :: TestEvent) -> s + 1)
                      0

    res `shouldSatisfy` either (const False) (const True)
    let Right st = res

    st `shouldBe` (10 :: Int)

  specify "API - Iterator.readAllEvents" $ do
    let events = fmap TestEvent [0..9]

    appendEvents store "readAllEvents" AnyVersion events

    res <- streamIterator store "readAllEvents"
    res `shouldSatisfy` isReadSuccess
    let ReadSuccess i = res

    got <- iteratorReadAllEvents i
    got `shouldBe` events
