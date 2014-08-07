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

module Database.PostgreSQL.Simple.Streams.Cursor
    ( openCursor
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
import           Database.PostgreSQL.Simple.Internal ( newTempName )

import           System.IO.Streams ( InputStream )
import qualified System.IO.Streams          as Streams
import qualified System.IO.Streams.Internal as Streams

-- | Query a database by opening a cursor on the postgresql backend
--   and creating an 'InputStream' to access that cursor on the frontend.
openCursor :: ( ToRow q, FromRow r )
           => Connection -> Query -> q -> IO ( InputStream r )
openCursor = openCursorWithOptions defaultFoldOptions

-- | Variant of 'openCursor' that does not perform parameter substitution.
openCursor_ :: ( FromRow r )
           => Connection -> Query -> IO ( InputStream r )
openCursor_ = openCursorWithOptions_ defaultFoldOptions

-- | Variant of 'openCursor' with additional options.   Note since this
--   requires manual transaction management,  so the 'transactionMode'
--   option is ignored.
openCursorWithOptions :: ( ToRow q, FromRow r )
                      => FoldOptions
                      -> Connection
                      -> Query -> q
                      -> IO ( Streams.InputStream r )
openCursorWithOptions opts conn template qs = do
  q <- formatQuery conn template qs
  openCursorWithOptions_ opts conn (Query q)

-- | Variant of 'openCursor' with additional options.   Note since this
--   requires manual transaction management,  so the 'transactionMode'
--   option is ignored.
openCursorWithOptions_ :: ( FromRow row )
                       => FoldOptions
                       -> Connection
                       -> Query
                       -> IO ( InputStream row )
openCursorWithOptions_ FoldOptions{..} conn q = do
    name <- declare q
    doneRef <- newIORef False
    rowsRef <- newIORef []
    return $! Streams.InputStream (read name doneRef rowsRef) (unRead rowsRef)
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
    chunkSize = case fetchQuantity of
                 Automatic   -> 256
                 Fixed n     -> n
