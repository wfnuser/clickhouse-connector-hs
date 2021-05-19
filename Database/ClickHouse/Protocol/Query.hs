{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Query where

import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Block
import qualified Database.ClickHouse.Protocol.ClientProtocol as CP
import Database.ClickHouse.Protocol.Const
import Database.ClickHouse.Protocol.Decoder
import Database.ClickHouse.Protocol.Encoder
import qualified Database.ClickHouse.Protocol.QueryProcessingStage as Stage
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
import Z.Data.ASCII
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P
import qualified Z.Data.Vector as V

queryBuilder :: V.Bytes -> V.Bytes -> B.Builder ()
queryBuilder q tablename = do
  encodeVarUInt CP._QUERY
  encodeBinaryStr "" -- query id
  -- TODO: should get client info from some where
  -- client info
  encodeVarUInt 1
  encodeBinaryStr "default"
  encodeBinaryStr "1"
  encodeBinaryStr "[::ffff:127.0.0.1]:0"
  encodeVarUInt 1 -- iface type TCP
  encodeBinaryStr ""
  encodeBinaryStr "macbook-pro-2.local" -- hostname
  encodeBinaryStr _CLIENT_NAME
  encodeVarUInt 21 -- client major version
  encodeVarUInt 6 -- client minor version
  encodeVarUInt 54448 -- client revision
  encodeBinaryStr "" -- quota key
  encodeVarUInt 1 -- client version patch

  -- 0 is a marker of the end of the settings
  encodeVarUInt 0

  encodeVarUInt Stage._COMPLETE
  encodeVarUInt 0 -- not compression
  encodeBinaryStr q

  emptyBlockBuilder tablename

sendQuery :: V.Bytes -> V.Bytes -> CHConn -> IO CHConn
sendQuery q tablename c = do
  let bytes = B.build $ queryBuilder q tablename
  -- print bytes
  chWrite c bytes
  return c