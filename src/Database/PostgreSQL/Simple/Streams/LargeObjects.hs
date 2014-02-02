-----------------------------------------------------------------------------
-- |
-- Module      :  Database.PostgreSQL.Simple.Streams.LargeObjects
-- Copyright   :  (c) 2013-2014 Leon P Smith
-- License     :  BSD3
--
-- Maintainer  :  leon@melding-monads.com
-- Stability   :  experimental
--
--
-- An io-stream based interface to importing and export large objects.
--
-- Note that these functions and the streams they create must be
-- used inside a single transaction,  and it is up to you to manage
-- that transaction.
--
-- You may interleave the use of these streams with other commands
-- on the same connection.
--
-----------------------------------------------------------------------------

{-# LANGUAGE BangPatterns #-}

module Database.PostgreSQL.Simple.Streams.LargeObjects
    ( loImport
    , loImportWithOid
    , loExport
    , loReadStream
    , loWriteStream
    ) where

import           Data.ByteString(ByteString)
import qualified Data.ByteString as BS

import           System.IO.Streams (InputStream, OutputStream)
import qualified System.IO.Streams as Streams

import qualified Database.PostgreSQL.Simple              as DB
import qualified Database.PostgreSQL.Simple.LargeObjects as DB


bUFSIZE :: Int
bUFSIZE = 4096

loExport :: DB.Connection -> DB.Oid -> IO (InputStream ByteString)
loExport conn oid = do
    lofd <- DB.loOpen conn oid DB.ReadMode
    loReadStream conn lofd bUFSIZE

loImport :: DB.Connection -> IO (DB.Oid, OutputStream ByteString)
loImport conn = do
    oid  <- DB.loCreat conn
    sOut <- loImportWithOid conn oid
    return (oid, sOut)

loImportWithOid :: DB.Connection -> DB.Oid -> IO (OutputStream ByteString)
loImportWithOid conn oid = do
    lofd <- DB.loOpen conn oid DB.WriteMode
    loWriteStream conn lofd

-- | @'loReadStream' conn lofd bufferSize@ returns an 'InputStream' that
--   reads chunks of size @bufferSize@ from the large object descriptor.
--
--   This stream may only be used in the context of a single explicit
--   transaction block.

loReadStream :: DB.Connection -> DB.LoFd -> Int -> IO (InputStream ByteString)
loReadStream conn lofd bufSize = do
    Streams.makeInputStream $ do
        x <- DB.loRead conn lofd bufSize
        return $! if BS.null x then Nothing else Just x

-- | @'loWriteStream' conn lofd@ returns an 'OutputStream' that
--   writes bytestrings to the large object descriptor.

loWriteStream :: DB.Connection -> DB.LoFd -> IO (OutputStream ByteString)
loWriteStream conn lofd = do
    Streams.makeOutputStream $ maybe (return ()) write
  where
    write !bs = do
      n <- DB.loWrite conn lofd bs
      if BS.length bs < n
        then write (BS.drop n bs)
        else return ()
