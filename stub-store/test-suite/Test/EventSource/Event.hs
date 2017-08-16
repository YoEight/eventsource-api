{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
--------------------------------------------------------------------------------
-- |
-- Module : Test.EventSource.Event
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module Test.EventSource.Event where

--------------------------------------------------------------------------------
import Data.Aeson.Types
import EventSource
import Protolude

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
