-----------------------------------------------------------------------------
-- |
-- Module      :  Database.PostgreSQL.Simple.Streams.Copy
-- Copyright   :  (c) 2014 Leon P Smith
-- License     :  BSD3
--
-- Maintainer  :  leon@melding-monads.com
-- Stability   :  experimental
--
--
-- An io-stream based interface to @COPY OUT@.
--
-- These operators do not need to be run in an explicit transaction,  however
-- it is necessary to fully consume the resulting stream before another
-- command can be issued on the same connection.
--
-- @COPY IN@ is not supported at the present time,  because some additional
-- interface design is needed to account for cancelling the operation via
-- 'putCopyError'.
--
-----------------------------------------------------------------------------

{-# LANGUAGE OverloadedStrings #-}

module Database.PostgreSQL.Simple.Streams.Copy
     ( copyOut
     , copyOut_
     ) where

import           Control.Exception
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Monoid
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Transaction
import           Database.PostgreSQL.Simple.Types
import           Database.PostgreSQL.Simple.Internal
import           Database.PostgreSQL.Simple.Copy
import qualified Database.PostgreSQL.LibPQ as PQ

import           System.IO.Streams ( InputStream )
import qualified System.IO.Streams          as Streams

{--
copyIn :: ( ToRow params ) => Connection -> Query -> params -> IO (OutputStream ByteString)
copyIn conn template qs = do
    q <- formatQuery conn template qs
    doCopyIn "Database.PostgreSQL.Simple.Streams.Copy.copyIn" conn template q

copyIn_ :: Connection -> Query -> IO (OutputStream ByteString)
copyIn_ conn (Query q) = do
    doCopyIn "Database.PostgreSQL.Simple.Streams.Copy.copyIn_" conn (Query q) q

doCopyIn :: B.ByteString -> Connection -> Query -> B.ByteString -> IO (OutputStream ByteString)
doCopyIn funcName conn template q = do
    result <- exec conn q
    status <- PQ.resultStatus result
    let err = throwIO $ QueryError
                  (B.unpack funcName ++ " " ++ show status)
                  template
    case status of
      PQ.EmptyQuery    -> err
      PQ.CommandOk     -> err
      PQ.TuplesOk      -> err
      PQ.CopyOut       -> err
      PQ.CopyIn        -> return ()
      PQ.BadResponse   -> throwResultError funcName result status
      PQ.NonfatalError -> throwResultError funcName result status
      PQ.FatalError    -> throwResultError funcName result status
    Streams.makeOutputStream $ \mx -> do
        case mx of
          Nothing -> putCopyEnd conn
          Just x  -> putCopyData conn x
--}


-- | Issue a @COPY TO STDOUT@ command and creates an 'InputStream' that
--   returns the resulting data.
copyOut :: ( ToRow params ) => Connection -> Query -> params -> IO (InputStream ByteString)
copyOut conn template qs = do
    q <- formatQuery conn template qs
    doCopyOut "Database.PostgreSQL.Simple.Streams.Copy.copyOut" conn template q


-- | Variant of 'copyOut' that does not perform parameter substitution.
copyOut_ :: Connection -> Query -> IO (InputStream ByteString)
copyOut_ conn (Query q) = do
    doCopyOut "Database.PostgreSQL.Simple.Streams.Copy.copyOut_" conn (Query q) q

doCopyOut :: B.ByteString -> Connection -> Query -> B.ByteString -> IO (InputStream ByteString)
doCopyOut funcName conn template q = do
    result <- exec conn q
    status <- PQ.resultStatus result
    let err = throwIO $ QueryError
                  (B.unpack funcName ++ " " ++ show status)
                  template
    case status of
      PQ.EmptyQuery    -> err
      PQ.CommandOk     -> err
      PQ.TuplesOk      -> err
      PQ.CopyOut       -> return ()
      PQ.CopyIn        -> err
      PQ.BadResponse   -> throwResultError funcName result status
      PQ.NonfatalError -> throwResultError funcName result status
      PQ.FatalError    -> throwResultError funcName result status
    Streams.makeInputStream $ do
      x <- getCopyData conn
      case x of
        CopyOutRow  row    -> return $! Just row
        CopyOutDone _count -> return Nothing
