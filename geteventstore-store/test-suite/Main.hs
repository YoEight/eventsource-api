--------------------------------------------------------------------------------
-- |
-- Module : Main
-- Copyright : (C) 2016 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
import qualified Test.Tasty

--------------------------------------------------------------------------------
import qualified Test.EventSource.Store.GetEventStore as GES

--------------------------------------------------------------------------------
main :: IO ()
main = do
    tree <- sequence [ GES.test ]
    Test.Tasty.defaultMain (Test.Tasty.testGroup "EventSource API" tree)
