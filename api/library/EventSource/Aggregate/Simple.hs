{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TypeFamilies           #-}
--------------------------------------------------------------------------------
-- |
-- Module    :  EventSource.Aggregate.Simple
-- Copyright :  (C) 2017 Yorick Laupa
-- License   :  (see the file LICENSE)
-- Maintainer:  Yorick Laupa <yo.eight@gmail.com>
-- Stability :  experimental
-- Portability: non-portable
--
--------------------------------------------------------------------------------
module EventSource.Aggregate.Simple where

--------------------------------------------------------------------------------
import Protolude

--------------------------------------------------------------------------------
import qualified EventSource.Aggregate as Self

--------------------------------------------------------------------------------
newtype Simple id event state = Simple state

--------------------------------------------------------------------------------
class AggregateIO event state | event -> state where
  applyIO :: state -> event -> IO state

--------------------------------------------------------------------------------
instance AggregateIO event state => Self.Aggregate (Simple id event state) where
  type Id  (Simple id event state) = id
  type Evt (Simple id event state) = event
  type M   (Simple id event state) = IO

  apply (Simple a) e = fmap Simple (applyIO a e)
