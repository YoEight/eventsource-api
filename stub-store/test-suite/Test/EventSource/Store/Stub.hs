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
import EventSource.Store.Stub
import Test.Tasty (TestTree)
import Test.Tasty.Hspec

--------------------------------------------------------------------------------
import Test.EventSource.Store.Specification

--------------------------------------------------------------------------------
test :: IO TestTree
test = testSpec "Store Stub" spec

--------------------------------------------------------------------------------
spec :: Spec
spec = parallel $ do
  stub <- runIO newStub
  specification stub
