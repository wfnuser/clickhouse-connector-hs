{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Insert where

import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Block
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

prepareInsert :: V.Bytes -> V.Bytes -> ServerInfo -> CHConn -> IO CHConn
prepareInsert tablename q info c = do
  let bytes = B.build $ queryBuilder q tablename
--   print bytes
  chWrite c bytes
  buf <- chRead c -- read meta data
--   print buf
  return c

sendEmptyBlock :: CHConn -> IO CHConn
sendEmptyBlock c = do
  let bytes = B.build $ emptyBlockBuilder ""
--   print bytes
  chWrite c bytes
  return c

sendData :: V.Bytes -> Block -> CHConn -> IO CHConn
sendData tablename block c = do
  let bytes = B.build $ blockBuilder tablename $ Just block
--   print bytes
  chWrite c bytes
  return c