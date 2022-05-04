{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NumericUnderscores  #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Plutus.ChainIndex.Events where

import Cardano.Api qualified as C
import Cardano.BM.Trace (Trace)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TBMQueue (flushTBMQueue, isFullTBMQueue)
import Control.Monad (forever, void)
import Data.Functor ((<&>))
import Data.Maybe (catMaybes)
import Ledger.Slot (Slot (Slot))
import Numeric.Natural (Natural)
import Plutus.ChainIndex qualified as CI
import Plutus.ChainIndex.Lib (ChainSyncEvent (Resume, RollBackward, RollForward), EventsQueue, RunRequirements,
                              runChainIndexDuringSync)
import Plutus.ChainIndex.SyncStats (SyncLog, logProgress)
import Plutus.Monitoring.Util (PrettyObject (PrettyObject), convertLog, runLogEffects)
import System.Clock (Clock (Monotonic), TimeSpec, diffTimeSpec, getTime)

-- | How often do we check the queue
period :: Int
period = 2_000_000 -- 2s


-- | checks if start local node slot has been reached
localSlotReached :: C.SlotNo -> CI.Tip -> Bool
localSlotReached (C.SlotNo sn) CI.TipAtGenesis        = sn == 0
localSlotReached (C.SlotNo sn) (CI.Tip (Slot cn) _ _) = cn >= fromIntegral sn

-- | We estimate the size of the event with the number of the transactions in the block.
-- By doing this we accumulate some number of blocks but with less than 'queueSize' number of transactions.
-- This approach helps to process blocks with a constant memory usage.
--
-- Just accumulating 'queueSize' blocks doesn't work as a block can have any number of transactions.
-- It works fine at the beginning of the chain but later blocks grow in their size and the memory
-- usage grows tremendously.
-- Patch: Return length of transaction number until local node tip reached
-- Otherwise returns max (length transaction) maxSize to ensure that tip is up to date

measureEventByTxs :: C.SlotNo -> Natural -> ChainSyncEvent -> Natural
measureEventByTxs startSlot maxSize (RollForward (CI.Block tip transactions) _) =
  let txLen = fromIntegral $ length transactions in
    if localSlotReached startSlot tip then max (maxSize + 1) txLen else txLen
measureEventByTxs _ maxSize _ = maxSize + 1 -- to handle resume and rollback asap

-- | 'processEventsQueue' reads events from 'TBQueue', collects enough 'RollForward's to
-- append blocks at once.
processEventsQueue :: Trace IO (PrettyObject SyncLog) -> RunRequirements -> EventsQueue -> IO ()
processEventsQueue trace runReq eventsQueue = forever $ do
  start <- getTime Monotonic
  eventsToProcess <- do
    let
      waitUntilEvents = do
        isFull <- atomically $ isFullTBMQueue eventsQueue
        if isFull then atomically $ flushTBMQueue eventsQueue
        else threadDelay period >> waitUntilEvents
    waitUntilEvents
  processEvents start eventsToProcess
  where
    processEvents :: TimeSpec -> [ChainSyncEvent] -> IO ()
    processEvents _ [] = pure ()
    processEvents start events@( e : restEvents ) = case e of
      (Resume resumePoint) -> do
        void $ runChainIndexDuringSync runReq $ CI.resumeSync resumePoint
        end <- getTime Monotonic
        void $ runLogEffects (convertLog PrettyObject trace) $ logProgress [e] (diffTimeSpec end start)
        processEvents start restEvents

      (RollBackward backwardPoint _) -> do
        void $ runChainIndexDuringSync runReq $ CI.rollback backwardPoint
        end <- getTime Monotonic
        void $ runLogEffects (convertLog PrettyObject trace) $ logProgress [e] (diffTimeSpec end start)
        processEvents start restEvents

      (RollForward _ _) -> do
         let isNotRollForwardEvt = \case
               (RollForward _ _) -> False
               _                 -> True
             (rollForwardEvents, restEvents') = break isNotRollForwardEvt events
             blocks = catMaybes $ rollForwardEvents <&> \case
               (RollForward block _) -> Just block
               _                     -> Nothing
         void $ runChainIndexDuringSync runReq $ CI.appendBlocks blocks
         end <- getTime Monotonic
         void $ runLogEffects (convertLog PrettyObject trace) $ logProgress rollForwardEvents (diffTimeSpec end start)
         processEvents start restEvents'
