-----------------------------------------------------------------------------
-- |
-- Module      :  Database.PostgreSQL.Simple.Streams.Cursor
-- Copyright   :  (c) 2013-2014 Leon P Smith
-- License     :  BSD3
--
-- Maintainer  :  leon@melding-monads.com
-- Stability   :  experimental
--
-- An io-streams interface to database queries.
--
-- These functions and the streams they create must be used inside a
-- single explicit transaction block.  Attempting to use a stream
-- after the transaction commits or rolls back will eventually result
-- in an exception.  It is up to you to manage that transaction,
-- unlike the 'fold' operator which creates a transaction block if
-- you are not already in one.
--
-- You may interleave the use of these streams with other commands
-- on the same connection.
--
-----------------------------------------------------------------------------

{-# LANGUAGE OverloadedStrings, RecordWildCards #-}
{-# LANGUAGE BangPatterns, DoAndIfThenElse #-}

module Database.PostgreSQL.Simple.Streams.Cursor
    ( withCursor
    , withCursor_
    , withCursorWithOptions
    , withCursorWithOptions_
    , openCursor
    , openCursor_
    , openCursorWithOptions
    , openCursorWithOptions_
    , FoldOptions(..)
    ) where

import           Prelude hiding (catch)
import           Blaze.ByteString.Builder
import           Blaze.Text

import           Control.Exception

import           Data.Monoid
import           Data.IORef

import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Transaction
import           Database.PostgreSQL.Simple.Types
import           Database.PostgreSQL.Simple.Internal
                     ( newTempName
                     , withConnection
                     )
import qualified Database.PostgreSQL.LibPQ as PQ

import           System.IO.Streams ( InputStream )
import qualified System.IO.Streams          as Streams
import qualified System.IO.Streams.Internal as Streams



-- | Open a cursor on a postgresql backend,  and present it as an 'InputStream'
--   for use within a definite scope. Because cursors need to be run inside a
--   transaction,  this function will detect whether or not the connection
--   is in a transaction,  and will create a transaction if needed.
withCursor :: ( ToRow q, FromRow r )
           => Connection -> Query -> q
           -> (InputStream r -> IO a) -> IO a
withCursor = withCursorWithOptions defaultFoldOptions

-- | A variant of 'withCursor' that does not perform parameter substitution.
withCursor_ :: ( FromRow r )
            => Connection -> Query
            -> (InputStream r -> IO a) -> IO a
withCursor_ = withCursorWithOptions_ defaultFoldOptions

-- | Variant of 'withCursor' with additional options.
withCursorWithOptions :: ( ToRow q, FromRow r )
                      => FoldOptions
                      -> Connection -> Query -> q
                      -> (InputStream r -> IO a) -> IO a
withCursorWithOptions opt conn template qs handle = do
    q <- formatQuery conn template qs
    withCursorWithOptions_ opt conn (Query q) handle

-- | Variant of 'withCursorwithOptions' that does not perform parameter
--   substitution.
withCursorWithOptions_ :: ( FromRow r )
                       => FoldOptions
                       -> Connection -> Query
                       -> (InputStream r -> IO a) -> IO a
withCursorWithOptions_ opt@FoldOptions{..} conn q handle = do
    status <- withConnection conn PQ.transactionStatus
    case status of
      PQ.TransIdle    -> do
          withTransactionMode transactionMode conn $ do
              (cursor,_cancel) <- openCursorWithOptions_ opt conn q
              handle cursor
      PQ.TransInTrans -> do
          bracket
              (openCursorWithOptions_ opt conn q)
              (\(_cursor, cancel) -> cancel)
              (\(cursor, _cancel) -> handle cursor)
      PQ.TransActive  -> fail "withCursorWithOptions_ FIXME:  PQ.TransActive"
         -- This _shouldn't_ occur in the current incarnation of
         -- the library,  as we aren't using libpq asynchronously.
         -- However,  it could occur in future incarnations of
         -- this library or if client code uses the Internal module
         -- to use raw libpq commands on postgresql-simple connections.
      PQ.TransInError -> fail "withCursorWithOptions_ FIXME:  PQ.TransInError"
         -- This should be turned into a better error message.
         -- It is probably a bad idea to automatically roll
         -- back the transaction and start another.
      PQ.TransUnknown -> fail "withCursorWithOptions_ FIXME:  PQ.TransUnknown"
         -- Not sure what this means.

-- | Query a database by opening a cursor on the postgresql backend
--   and creating an 'InputStream' to access that cursor on the frontend.
--   Also returns an action that can be used to free the cursor.
openCursor :: ( ToRow q, FromRow r )
           => Connection -> Query -> q -> IO ( InputStream r, IO () )
openCursor = openCursorWithOptions defaultFoldOptions

-- | Variant of 'openCursor' that does not perform parameter substitution.
openCursor_ :: ( FromRow r )
           => Connection -> Query -> IO ( InputStream r, IO () )
openCursor_ = openCursorWithOptions_ defaultFoldOptions

-- | Variant of 'openCursor' with additional options.   Note since this
--   requires manual transaction management,  so the 'transactionMode'
--   option is ignored.
openCursorWithOptions :: ( ToRow q, FromRow r )
                      => FoldOptions
                      -> Connection
                      -> Query -> q
                      -> IO ( InputStream r, IO () )
openCursorWithOptions opts conn template qs = do
  q <- formatQuery conn template qs
  openCursorWithOptions_ opts conn (Query q)

-- | Variant of 'openCursor' with additional options.   Note since this
--   requires manual transaction management,  so the 'transactionMode'
--   option is ignored.
openCursorWithOptions_ :: ( FromRow r )
                       => FoldOptions
                       -> Connection
                       -> Query
                       -> IO ( InputStream r,  IO () )
openCursorWithOptions_ FoldOptions{..} conn q = do
    name <- declare q
    doneRef <- newIORef False
    rowsRef <- newIORef []
    let !inS = Streams.InputStream (read name doneRef rowsRef) (unRead rowsRef)
        !cancl = cancel doneRef name
    return $! ( inS, cancl )
  where
    read name doneRef rowsRef = do
        l <- readIORef rowsRef
        case l of
          [] -> do
              done <- readIORef doneRef
              if done
                then return Nothing
                else do
                    rows <- fetch name
                    case rows of
                      [] -> do
                          writeIORef doneRef True
                          close name
                          return Nothing
                      (row:rows') -> do
                          writeIORef rowsRef rows'
                          return $! Just row
          (row:rows') -> do
              writeIORef rowsRef rows'
              return $! Just row
    unRead ref x = readIORef ref >>= \xs -> writeIORef ref (x:xs)
    declare q = do
        name <- newTempName conn
        _ <- execute_ conn $ mconcat
                 [ "DECLARE ", name, " NO SCROLL CURSOR FOR ", q ]
        return name
    fetch (Query name) = query_ conn $
        Query (toByteString (fromByteString "FETCH FORWARD "
                             `mappend` integral chunkSize
                             `mappend` fromByteString " FROM "
                             `mappend` fromByteString name
                            ))
    close name =
        (execute_ conn ("CLOSE " `mappend` name) >> return ()) `catch` \ex ->
            -- Don't throw exception if CLOSE failed because the transaction is
            -- aborted.  Otherwise, it will throw away the original error.
            if isFailedTransactionError ex then return () else throwIO ex
    cancel doneRef name = do
        done <- atomicModifyIORef doneRef (\x -> (True, x))
        if done
        then return ()
        else close name
    chunkSize = case fetchQuantity of
                 Automatic   -> 256
                 Fixed n     -> n
