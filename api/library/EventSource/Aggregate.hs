{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs            #-}
{-# LANGUAGE Rank2Types       #-}
{-# LANGUAGE TypeFamilies     #-}
--------------------------------------------------------------------------------
-- |
-- Module    :  EventSource.Aggregate
-- Copyright :  (C) 2017 Yorick Laupa
-- License   :  (see the file LICENSE)
-- Maintainer:  Yorick Laupa <yo.eight@gmail.com>
-- Stability :  experimental
-- Portability: non-portable
--
-- Implementation of an aggregate abstraction.
-- Link: https://en.wikipedia.org/wiki/Domain-driven_design.
--------------------------------------------------------------------------------
module EventSource.Aggregate
  ( StreamId(..)
  -- * Aggregate
  , Aggregate(..)
  , Validate(..)
  , Decision
  , Agg
  , aggId
  , runAgg
  , newAgg
  , loadAgg
  , loadOrCreateAgg
  -- * Interactions
  , submitCmd
  , submitEvt
  , snapshot
  , execute
  -- * Internal
  , Action'(..)
  , Action
  , askEnv
  , getState
  , putState
  , AggEnv(..)
  , AggState(..)
  , persist
  ) where

--------------------------------------------------------------------------------
import Control.Monad (ap,forever)
import Data.Foldable (for_)

--------------------------------------------------------------------------------
import           Control.Concurrent.Async.Lifted (wait)
import qualified Control.Concurrent.Lifted as Concurrent
import           Control.Monad.Base (MonadBase, liftBase)
import           Control.Monad.Except (runExceptT)
import           Control.Monad.Trans (MonadTrans(..))
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Data.IORef.Lifted (newIORef, readIORef, writeIORef)

--------------------------------------------------------------------------------
import EventSource

--------------------------------------------------------------------------------
-- | Maps an id to a 'StreamName'.
class StreamId a where
  toStreamName :: a -> StreamName

--------------------------------------------------------------------------------
-- | Represents a stream aggregate. An aggregate can rebuild its internal state
--   by replaying all the stream's events that aggregate is responsible for.
class Aggregate a where
  -- | Type of the id associated to the aggregate.
  type Id a  :: *

  -- | Type of event handled by the aggregate.
  type Evt a :: *

  -- | Type of monad stack used by the aggregate.
  type M a :: * -> *

  -- | Given current aggregate state, updates it according to the event the
  --   aggregate receives.
  apply :: a -> Evt a -> M a a

--------------------------------------------------------------------------------
-- | When validating a command, tells if the command was valid. If the command
--   is valid, it returns an event. Otherwise, it returns an error.
type Decision a = Either (Err a) (Evt a)

--------------------------------------------------------------------------------
-- | Represents an aggregate that support validation. An aggregate that supports
--   validation can receive command and decide if it was valid or not. When the
--   validation is successful, The aggregate emits an event that will be
--   persisted and pass to 'apply' function.
class Aggregate a => Validate a where
  -- | Type of command supported by the aggregate.
  type Cmd a :: *

  -- | Type of error that aggregate can yield.
  type Err a :: *

  -- | Validates a command. If the command validation succeeds, it will emits
  --   an event. Otherwise, it will returns an error.
  validate :: a -> Cmd a -> M a (Decision a)

--------------------------------------------------------------------------------
-- | Internal aggregate action. An action is executed by an aggregate. An action
--   embodies fundamental operations like submitting event, validating command
--   or returning the current snapshot of an aggregate. Action are CPS-ed
--   encoded so the execution model can be flexible. An action can perform
--   synchronously or asynchronously.
newtype Action' e s m a =
  Action' { runAction :: e
                      -> s
                      -> (s -> a -> m ())
                      -> m () }

--------------------------------------------------------------------------------
instance Functor (Action' e s m) where
  fmap f (Action' k) = Action' $ \e s resp ->
    k e s (\s' a -> resp s' (f a))

--------------------------------------------------------------------------------
instance Applicative (Action' e s m) where
  pure  = return
  (<*>) = ap

--------------------------------------------------------------------------------
instance Monad (Action' e s m) where
  return a = Action' $ \_ s resp -> resp s a

  Action' k >>= f = Action' $ \e s resp ->
    k e s (\s' a -> runAction (f a) e s' resp)

--------------------------------------------------------------------------------
instance MonadTrans (Action' e s) where
  lift m = Action' $ \_ s resp -> m >>= resp s

--------------------------------------------------------------------------------
-- | Returns an action environment.
askEnv :: Action' e s m e
askEnv = Action' $ \e s resp -> resp s e

--------------------------------------------------------------------------------
-- | Returns an action current state.
getState :: Action' e s m s
getState = Action' $ \_ s resp -> resp s s

--------------------------------------------------------------------------------
-- | Set an action state.
putState :: s -> Action' e s m ()
putState s = Action' $ \_ _ resp -> resp s ()

--------------------------------------------------------------------------------
-- | An action configured to aggregate internal types.
type Action a r = Action' (AggEnv a) (AggState a) (M a) r

--------------------------------------------------------------------------------
-- | Aggregate internal environment.
data AggEnv a =
  AggEnv { aggEnvStore :: SomeStore
           -- ^ Handle to an eventstore.
         , aggEnvId :: Id a
           -- ^ Identification of the aggregate.
         }

--------------------------------------------------------------------------------
-- | Aggregate internal state.
data AggState a =
  AggState { aggStateVersion :: !ExpectedVersion
             -- ^ Expected version of the next write. This isn't expose to
             --   user and it's updated automatically.
           , aggState :: !a
             -- ^ Aggregate current state.
           }

--------------------------------------------------------------------------------
-- | A stream aggregate. An aggregate updates its internal based on the event
--  it receives. You can read its current state by using 'snapshot'. If it
--  supports validation, through 'Validated' typeclass, it can receive
--  command and emits an event if the command was successful. Otherwise, it will
--  yield an error. When receiving valid command, an aggregate will persist the
--  resulting event. An aggregate is only responsible of its own stream.
data Agg a where
  Agg :: AggEnv a
      -> (forall r. Action a r -> (r -> M a ()) -> M a ())
      -> Agg a

--------------------------------------------------------------------------------
-- | Returns an aggregate id.
aggId :: Agg a -> Id a
aggId (Agg env _) = aggEnvId env

--------------------------------------------------------------------------------
-- | Executes an action on an aggregate.
runAgg :: Agg a -> Action a r -> (r -> M a ()) -> M a ()
runAgg (Agg _ k) = k

--------------------------------------------------------------------------------
-- | Holds an existantially quantified action so it can passed around easily
--   to aggregate's internal concurrent channel.
data Msg a where
  Msg :: Action a r -> (r -> M a ()) -> Msg a

--------------------------------------------------------------------------------
-- | Creates a new aggregate given an eventstore handle, an id and an initial
--   state.
newAgg :: (Aggregate a, MonadBaseControl IO (M a))
       => SomeStore
       -> Id a
       -> a
       -> M a (Agg a)
newAgg store aId seed = do
  let env = AggEnv store aId
  ref  <- newIORef (AggState AnyVersion seed)
  chan <- Concurrent.newChan
  _    <- Concurrent.fork $ forever $ do
    Msg action k <- Concurrent.readChan chan
    s            <- readIORef ref
    runAction action env s $ \s' r -> do
      writeIORef ref s'
      k r

  pure $ Agg env $ \action k ->
    Concurrent.writeChan chan (Msg action k)

--------------------------------------------------------------------------------
-- | Creates an aggregate and replays its entire stream to rebuild its
--   internal state.
loadAgg :: (Aggregate a, StreamId (Id a), DecodeEvent (Evt a), MonadBaseControl IO (M a))
        => SomeStore
        -> Id a
        -> a
        -> M a (Either ForEventFailure (Agg a))
loadAgg store aId seed = do
  agg <- newAgg store aId seed
  res <- execute agg (loadEventsAction aId)

  pure (agg <$ res)

--------------------------------------------------------------------------------
-- | Like 'loadAgg' but call 'loadAgg' in case of 'ForEventFailure' error.
loadOrCreateAgg :: (Aggregate a, StreamId (Id a), DecodeEvent (Evt a), MonadBaseControl IO (M a))
                => SomeStore
                -> Id a
                -> a
                -> M a (Agg a)
loadOrCreateAgg store aId seed = do
  agg <- newAgg store aId seed
  _   <- execute agg (loadEventsAction aId)
  pure agg

--------------------------------------------------------------------------------
-- | Submits a command to the aggregate. If the command was valid, it returns
-- an event otherwise an error. In case of a valid command, the aggregate
-- persist the resulting event to the eventstore. The aggregate will also
-- update its internal state accordingly.
submitCmd :: (Validate a, MonadBase IO (M a), StreamId (Id a), EncodeEvent (Evt a))
          => Agg a
          -> Cmd a
          -> M a (Decision a)
submitCmd agg cmd = execute agg (submitCmdAction cmd)

--------------------------------------------------------------------------------
-- | Submits an event. The aggregate will update its internal state accondingly.
submitEvt :: (Aggregate a, MonadBase IO (M a)) => Agg a -> Evt a -> M a ()
submitEvt agg evt = execute agg (submitEvtAction evt)

--------------------------------------------------------------------------------
-- | Returns current aggregate state.
snapshot :: MonadBase IO (M a) => Agg a -> M a a
snapshot agg = execute agg snapshotAction

--------------------------------------------------------------------------------
-- | Executes an action.
execute :: MonadBase IO (M a) => Agg a -> Action a r -> M a r
execute agg action = do
  var <- Concurrent.newEmptyMVar
  runAgg agg action (Concurrent.putMVar var)
  Concurrent.takeMVar var

--------------------------------------------------------------------------------
-- | Persist an event to the eventstore.
persist :: (StreamId id, EncodeEvent event, MonadBase IO m)
        => SomeStore
        -> id
        -> ExpectedVersion
        -> event
        -> m EventNumber
persist store aid ver event =
  liftBase (appendEvent store (toStreamName aid) ver event >>= wait)

--------------------------------------------------------------------------------
-- // Internal commands.
--------------------------------------------------------------------------------
submitCmdAction :: (Validate a, MonadBase IO (M a), StreamId (Id a), EncodeEvent (Evt a))
                => Cmd a
                -> Action a (Decision a)
submitCmdAction cmd = do
  env    <- askEnv
  s      <- getState
  result <- lift $ validate (aggState s) cmd

  for_ result $ \event ->
    do next <- lift $ persist (aggEnvStore env)
                              (aggEnvId env)
                              (aggStateVersion s)
                              event

       let s' = s { aggStateVersion = ExactVersion next }

       putState s'
       submitEvtAction event

  pure result

--------------------------------------------------------------------------------
submitEvtAction :: (Aggregate a, Monad (M a)) => Evt a -> Action a ()
submitEvtAction event = do
  s  <- getState
  a' <- lift $ apply (aggState s) event
  let s' = s { aggState = a' }
  putState s'

--------------------------------------------------------------------------------
snapshotAction :: Monad (M a) => Action a a
snapshotAction = fmap aggState getState

--------------------------------------------------------------------------------
loadEventsAction :: (Aggregate a, StreamId (Id a), DecodeEvent (Evt a), MonadBase IO (M a))
                 => Id a
                 -> Action a (Either ForEventFailure a)
loadEventsAction aId = do
  seed <- getState
  env  <- askEnv

  let go num s event =
        do a <- apply (aggState s) event
           pure s { aggState        = a
                  , aggStateVersion = ExactVersion (num + 1)
                  }

  lift $ runExceptT $ fmap aggState
       $ foldEventsWithNumberM (aggEnvStore env) (toStreamName aId) go seed
