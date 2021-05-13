{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Const where

import qualified Z.Data.Vector as V

-- Name, version, revision, default DB
_HELLO :: Word
_HELLO = 0 :: Word

-- Query id, settings,
_QUERY :: Word
_QUERY = 1 :: Word

-- A block of data
_DATA :: Word
_DATA = 2 :: Word

-- Cancel the query execution
_CANCEL :: Word
_CANCEL = 3 :: Word

-- Check that the connection to the server is alive
_PING :: Word
_PING = 4 :: Word

_DBMS_NAME :: V.Bytes
_DBMS_NAME = "ClickHouse" :: V.Bytes

{-# INLINE _CLIENT_NAME #-}
_CLIENT_NAME :: V.Bytes
_CLIENT_NAME = "haskell-driver" :: V.Bytes

{-# INLINE _CLIENT_VERSION_MAJOR #-}
_CLIENT_VERSION_MAJOR :: Word
_CLIENT_VERSION_MAJOR = 18 :: Word

{-# INLINE _CLIENT_VERSION_MINOR #-}
_CLIENT_VERSION_MINOR :: Word
_CLIENT_VERSION_MINOR = 10 :: Word

{-# INLINE _CLIENT_VERSION_PATCH #-}
_CLIENT_VERSION_PATCH :: Word
_CLIENT_VERSION_PATCH = 3 :: Word

{-# INLINE _CLIENT_REVISION #-}
_CLIENT_REVISION :: Word
_CLIENT_REVISION = 54429 :: Word
