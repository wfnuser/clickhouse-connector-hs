module Database.ClickHouse.Protocol.ClientProtocol where

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

_TABLES_STATUS_REQUEST :: Word
_TABLES_STATUS_REQUEST = 5 :: Word