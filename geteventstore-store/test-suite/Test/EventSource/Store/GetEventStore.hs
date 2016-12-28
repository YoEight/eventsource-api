{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
--------------------------------------------------------------------------------
-- |
-- Module : Test.EventSource.Store.GetEventStore
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module Test.EventSource.Store.GetEventStore (test) where

--------------------------------------------------------------------------------
import ClassyPrelude
import Database.EventStore
import EventSource.Store.GetEventStore
import Test.Tasty (TestTree)
import Test.Tasty.Hspec

--------------------------------------------------------------------------------
import Test.EventSource.Store.Specification

--------------------------------------------------------------------------------
test :: IO TestTree
test = testSpec "Store GetEventStore" spec

--------------------------------------------------------------------------------
spec :: Spec
spec = parallel $ do
  ges <- runIO $ gesStore defaultSettings (Static "127.0.0.1" 1113)
  specification ges
