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
-- An io-stream based interface to @COPY@.
--
-- These operators do not need to be run in an explicit transaction,  however
-- it is necessary to fully consume the resulting stream before another
-- command can be issued on the same connection.
--
-----------------------------------------------------------------------------

{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module Database.PostgreSQL.Simple.Streams.Copy
     ( withCopyIn
     , withCopyIn_
     , copyIn
     , copyIn_
     , copyOut
     , copyOut_
     ) where

import           Prelude hiding (catch)
import           Control.Concurrent.MVar
import           Control.Exception
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           Database.PostgreSQL.Simple.Internal
import           Database.PostgreSQL.Simple.Copy
import qualified Database.PostgreSQL.LibPQ as PQ

import           System.IO.Streams ( InputStream, OutputStream )
import qualified System.IO.Streams as Streams

-- | This wraps a call to 'copyIn' so that the copy will be committed
--   with 'putCopyEnd' upon a normal exit or cancelled with 'putCopyError'
--   upon a exception.  Thus, outside of the block, the connection will be
--   in a ready state and the 'OutputStream' will silently discard any
--   future writes.
--
--   Upon an abort, the error message passed to 'putCopyError' will be
--   the 'show' of the exception.

withCopyIn :: ( ToRow params ) => Connection -> Query -> params -> (OutputStream ByteString -> IO a) -> IO a
withCopyIn conn template qs thing = mask $ \restore -> do
    (outS, abort) <- copyIn conn template qs
    a <- restore (thing outS) `catch` \e -> do
             abort (B.pack (show e)) `catch` \(_ :: SomeException) -> return ()
             throwIO (e :: SomeException)
    Streams.write Nothing outS
    return a

-- | Variant of 'withCopyIn' that does not perform parameter substitution.

withCopyIn_ :: Connection -> Query -> (OutputStream ByteString -> IO a) -> IO a
withCopyIn_ conn query thing = mask $ \restore -> do
    (outS, abort) <- copyIn_ conn query
    a <- restore (thing outS) `catch` \e -> do
             abort (B.pack (show e)) `catch` \(_ :: SomeException) -> return ()
             throwIO (e :: SomeException)
    Streams.write Nothing outS
    return a

-- | Issues a @COPY FROM STDIN@ command and creates an 'OutputStream' for
--   writing to the database.   Also returns a command for cancelling the
--   query that is conceptually the same as 'putCopyError'.
--
--   The stream will issue 'putCopyEnd' when a 'Nothing' value is written
--   to it.   Once either the cancelling function has been called,
--   or 'Nothing' has been written,  the @Connection@ will have reverted
--   to the ready state,  the OutputStream will silently discard any
--   further writes, and calls to the cancel function will have no effect.
--
--   If you call @putCopyEnd@ or @putCopyError@ yourself on the connection,
--   then the stream will not know what state the connection is in,  and
--   an exception from calling a function on the connection while it
--   is in the wrong state is likely to result.

copyIn :: ( ToRow params ) => Connection -> Query -> params -> IO (OutputStream ByteString, ByteString -> IO ())
copyIn conn template qs = do
    q <- formatQuery conn template qs
    doCopyIn "Database.PostgreSQL.Simple.Streams.Copy.copyIn" conn template q

-- | Variant of 'copyIn' that does not perform parameter substitution.

copyIn_ :: Connection -> Query -> IO (OutputStream ByteString, ByteString -> IO ())
copyIn_ conn (Query q) = do
    doCopyIn "Database.PostgreSQL.Simple.Streams.Copy.copyIn_" conn (Query q) q

doCopyIn :: B.ByteString -> Connection -> Query -> B.ByteString -> IO (OutputStream ByteString, ByteString -> IO ())
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
    doneRef <- newMVar False
    outS <- Streams.makeOutputStream $ \mx -> do
               case mx of
                 Nothing -> do
                    done <- swapMVar doneRef True
                    if done
                      then return ()
                      else putCopyEnd conn >> return ()
                 Just x  -> do
                    withMVar doneRef $ \done -> do
                      if done
                        then return ()
                        else putCopyData conn x >> return ()
    let abort msg = do
          done <- swapMVar doneRef True
          if done
            then return ()
            else putCopyError conn msg
    return (outS, abort)

-- It might be nice to implement withCopyOut such that the copy is cancelled
-- if it's still ongoing after the block is exited.

-- | Issues a @COPY TO STDOUT@ command and creates an 'InputStream' that
--   returns the resulting data.
--
--   Note that the @InputStream@ must be fully consumed in order for the
--   connection to be usable for anything else.

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
