{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Block where

import Database.ClickHouse.Connection
import qualified Database.ClickHouse.Protocol.ClientProtocol as CP
import Database.ClickHouse.Protocol.Const
import Database.ClickHouse.Protocol.Decoder
import Database.ClickHouse.Protocol.Encoder
import Database.ClickHouse.Protocol.Packet
import Database.ClickHouse.Protocol.Query
import qualified Database.ClickHouse.Protocol.QueryProcessingStage as Stage
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
import Z.Data.ASCII
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import qualified Z.Data.Vector as V

blockBuilder :: V.Bytes -> V.Bytes -> ServerInfo -> B.Builder ()
blockBuilder tablename q info = do
  queryBuilder q
  writeBlock tablename info

writeBlock :: V.Bytes -> ServerInfo -> B.Builder ()
writeBlock tableName info = do
  encodeVarUInt CP._DATA
  encodeBinaryStr tableName

  blockServerInfoBuilder info

blockServerInfoBuilder :: ServerInfo -> B.Builder ()
blockServerInfoBuilder info = do
  encodeVarUInt 1
  encodeVarUInt 0 -- not over flows
  encodeVarUInt 2

  -- support bucket num later
  encodeVarInt32 (-1)
  encodeVarUInt 0

  -- write empty columns and rows
  encodeVarUInt 0 -- #col
  encodeVarUInt 0 -- #row

writeBlockStart :: V.Bytes -> V.Bytes -> ServerInfo -> CHConn -> IO CHConn
writeBlockStart tablename q info c = do
  let bytes = B.build $ blockBuilder tablename q info
  print bytes
  chWrite c bytes
  buf <- chRead c
  print buf
  return c

-- sendQuery :: V.Bytes -> CHConn -> IO CHConn
-- sendQuery q c = do
--   let bytes = B.build $ queryBuilder q
--   chWrite c bytes
--   return c