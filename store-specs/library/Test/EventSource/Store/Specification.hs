{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
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
import Control.Exception (Exception, toException, fromException)
import Control.Monad (unless)
import Data.Foldable (for_, traverse_)
import Data.Semigroup ((<>))

--------------------------------------------------------------------------------
import           Control.Concurrent.Async (wait)
import           Control.Monad.Except (runExceptT, mapExceptT)
import           Control.Monad.Base (MonadBase, liftBase)
import           Control.Monad.State (evalStateT, get, modify)
import           Data.Aeson.Types (object, withObject, (.=), (.:))
import           Data.Text (Text)
import           Data.UUID (toText)
import           Data.UUID.V4 (nextRandom)
import           EventSource
import           EventSource.Aggregate (StreamId(..))
import qualified EventSource.Aggregate.Simple as Simple
import           Test.Tasty.Hspec

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
newtype Test = Test Int deriving (Eq, Show)

--------------------------------------------------------------------------------
data TestCmd
  = TestIncr Int
  | TestCmdError

--------------------------------------------------------------------------------
data TestError = TestError deriving (Eq, Show)

--------------------------------------------------------------------------------
instance Exception TestError

--------------------------------------------------------------------------------
newtype TestId = TestId Text

--------------------------------------------------------------------------------
instance StreamId TestId where
  toStreamName (TestId i) = StreamName $ "test:stream:" <> i

--------------------------------------------------------------------------------
instance Simple.AggregateIO TestEvent Test where
  applyIO (Test x) (TestEvent i) = pure (Test (x+i))

--------------------------------------------------------------------------------
instance Simple.ValidateIO TestCmd TestEvent Test where
  validateIO _ cmd =
    case cmd of
      TestIncr i   -> pure (Right $ TestEvent i)
      TestCmdError -> pure (Left $ toException TestError)

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

  specify "API/Aggregate - submit event" $ do
    agg <- Simple.newAgg (toStore store) (TestId "submit:event") (Test 0)
    let events = replicate 10 (TestEvent 1)

    traverse_ (Simple.submitEvt agg) events
    got <- Simple.snapshot agg

    got `shouldBe` Test 10

  specify "API/Aggregate - submit commands" $ do
    agg <- Simple.newAgg (toStore store) (TestId "submit:command") (Test 0)

    res1 <- Simple.submitCmd agg (TestIncr 1)
    let go1 (Right evt) = evt == TestEvent 1
        go1 Left{}      = False
    res1 `shouldSatisfy` go1

    s1 <- Simple.snapshot agg
    s1 `shouldBe` Test 1

    res2 <- Simple.submitCmd agg TestCmdError
    let go2 Right{}  = False
        go2 (Left e) = fromException e == Just TestError
    res2 `shouldSatisfy` go2

  specify "API/Aggregate - loading" $ do
    agg1 <- Simple.newAgg (toStore store) (TestId "submit:load") (Test 0)

    let commands = replicate 10 (TestIncr 1)

    traverse_ (Simple.submitCmd agg1) commands

    res1 <- Simple.snapshot agg1
    res1 `shouldBe` Test 10

    outcome <- Simple.loadAgg (toStore store) (TestId "submit:load") (Test 0)

    case outcome of
      Left{}     -> error "We should be able to load an aggregate."
      Right agg2 ->
        do res2 <- Simple.snapshot agg2
           res2 `shouldBe` res1

           res3 <- Simple.submitCmd agg2 (TestIncr 1)
           let go3 Left{} = False
               go3 Right{} = True
           res3 `shouldSatisfy` go3

           res4 <- Simple.snapshot agg2
           res4 `shouldBe` Test 11

