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
{-# LANGUAGE CPP, BangPatterns, DoAndIfThenElse #-}

module Database.PostgreSQL.Simple.Streams.Copy
     ( withCopyIn
     , withCopyIn_
     , copyIn
     , copyIn_
     , withCopyOut
     , withCopyOut_
     , copyOut
     , copyOut_
     ) where

import           Prelude hiding (catch)
import           Control.Concurrent ( threadWaitRead )
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad ( void )
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.IORef
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           Database.PostgreSQL.Simple.Internal
import           Database.PostgreSQL.Simple.Copy
import qualified Database.PostgreSQL.LibPQ as PQ

import           System.IO.Streams ( InputStream, OutputStream )
import           System.IO.Streams.Internal ( InputStream(..) )
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

-- | Issues a @COPY TO STDOUT@ command and creates an 'InputStream' that
--   returns the resulting data.
--
--   This function ensures that the `InputStream` has been either fully
--   consumed or cancelled after the control flow exits the enclosing
--   block.   Note that cancelling this operation is a fairly expensive
--   operation,  so it is often preferable to fully consume the stream
--   instead.
withCopyOut :: ( ToRow params )
            => Connection -> Query -> params
            -> (InputStream ByteString -> IO a) -> IO a
withCopyOut conn template qs thing = do
    q <- formatQuery conn template qs
    bracket
        (doCopyOut funcName conn template q)
        snd
        (thing . fst)
  where
    funcName = "Database.PostgreSQL.Simple.Streams.Copy.withCopyOut"

-- | A variant of `withCopyOut` that does not perform parameter substitution.
withCopyOut_ :: Connection -> Query
             -> (InputStream ByteString -> IO a) -> IO a
withCopyOut_ conn (Query q) thing = do
    bracket
        (doCopyOut funcName conn (Query q) q)
        snd
        (thing . fst)
  where
    funcName = "Database.PostgreSQL.Simple.Streams.Copy.withCopyOut_"

-- | Issues a @COPY TO STDOUT@ command and creates an 'InputStream' that
--   returns the resulting data.
--
--   Note that the @InputStream@ must be fully consumed in order for the
--   connection to be usable for anything else.
--
--   The function also returns an IO action that can be used to cancel
--   the operation;  however note that cancelling the operation is fairly
--   expensive,  as it involves opening another connection to the postgres
--   database.   Thus, if the number of rows remaining is not large,
--   it may make sense to run the stream to completion instead.
copyOut :: ( ToRow params )
        => Connection -> Query -> params
        -> IO (InputStream ByteString, IO ())
copyOut conn template qs = do
    q <- formatQuery conn template qs
    doCopyOut "Database.PostgreSQL.Simple.Streams.Copy.copyOut" conn template q

-- | Variant of 'copyOut' that does not perform parameter substitution.
copyOut_ :: Connection -> Query -> IO (InputStream ByteString, IO ())
copyOut_ conn (Query q) = do
    doCopyOut "Database.PostgreSQL.Simple.Streams.Copy.copyOut_" conn (Query q) q

doCopyOut :: B.ByteString -> Connection -> Query -> B.ByteString -> IO (InputStream ByteString, IO ())
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
    doneRef <- newMVar False
    pbRef   <- newIORef []

    let pb x = readIORef pbRef >>= \xs -> writeIORef pbRef (x:xs)

    let grab = do
          rs <- readIORef pbRef
          case rs of
            [] -> do
                modifyMVar doneRef $ \done ->
                    if done
                    then return (True, Nothing)
                    else do
                      x <- getCopyData conn
                      case x of
                        CopyOutRow row     -> return (False, Just row)
                        CopyOutDone _count -> return (True,  Nothing )
            (r:rs') -> do
                writeIORef pbRef rs'
                return $! Just r

    let cancel = do
          done <- swapMVar doneRef True
          if done
          then return ()
          else do
            withConnection conn $ \c -> do
              status <- PQ.transactionStatus c
              case status of
                PQ.TransActive -> do
                    PQ.getCancel c >>= maybe (return ()) (void . PQ.cancel)
                    discardCopyData funcName c
                _ -> do
                    return ()

    let !inS = InputStream grab pb

    return $! ( inS, cancel )

discardCopyData :: B.ByteString -> PQ.Connection -> IO ()
discardCopyData funcName pqconn = loop
  where
    loop = do
#if defined(mingw32_HOST_OS)
      row <- PQ.getCopyData pqconn False
#else
      row <- PQ.getCopyData pqconn True
#endif
      case row of
        PQ.CopyOutRow _rowdata -> loop
        PQ.CopyOutDone -> return ()
#if defined(mingw32_HOST_OS)
        PQ.CopyOutWouldBlock -> do
            fail (B.unpack funcName ++ ": the impossible happened")
#else
        PQ.CopyOutWouldBlock -> do
            mfd <- PQ.socket pqconn
            case mfd of
              Nothing -> throwIO (fdError funcName)
              Just fd -> do
                  threadWaitRead fd
                  _ <- PQ.consumeInput pqconn
                  loop
#endif
        PQ.CopyOutError -> do
            mmsg <- PQ.errorMessage pqconn
            throwIO SqlError {
                          sqlState       = "",
                          sqlExecStatus  = FatalError,
                          sqlErrorMsg    = maybe "" id mmsg,
                          sqlErrorDetail = "",
                          sqlErrorHint   = funcName
                        }
