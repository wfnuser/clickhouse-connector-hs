{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Block where

import Data.Int
import qualified Database.ClickHouse.Protocol.ClientProtocol as CP
import Database.ClickHouse.Protocol.Const
import Database.ClickHouse.Protocol.Decoder
import Database.ClickHouse.Protocol.Encoder
import Database.ClickHouse.Protocol.Packet
import qualified Database.ClickHouse.Protocol.QueryProcessingStage as Stage
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
import Z.Data.ASCII
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import qualified Z.Data.Vector as V

data BlockInfo = Info
  { is_overflows :: !Bool,
    bucket_num :: {-# UNPACK #-} !Int32
  }
  deriving (Show)

defaultBlockInfo :: BlockInfo
defaultBlockInfo = Info False 0

data CKType
  = CKInt16 Int16
  | CKString V.Bytes
  deriving (Show)

data Block = ColumnOrientedBlock
  { columns_with_type :: V.Vector (V.Bytes, V.Bytes),
    blockdata :: V.Vector (V.Vector CKType),
    info :: BlockInfo
  }
  deriving (Show)

emptyBlockBuilder :: V.Bytes -> B.Builder ()
emptyBlockBuilder tableName = do
  blockBuilder tableName Nothing

blockBuilder :: V.Bytes -> Maybe Block -> B.Builder ()
blockBuilder tableName block = do
  encodeVarUInt CP._DATA
  encodeBinaryStr tableName
  blockInfoBuilder defaultBlockInfo

  case block of
    Nothing -> do
      -- write empty columns and rows
      encodeVarUInt 0 -- #col
      encodeVarUInt 0 -- #row
    Just (ColumnOrientedBlock columns_with_type blockdata info) -> do
      encodeVarUInt . fromIntegral . V.length $ blockdata
      encodeVarUInt . fromIntegral . V.length $ V.index blockdata 0


blockInfoBuilder :: BlockInfo -> B.Builder ()
blockInfoBuilder (Info is_overflows bucket_num) = do
  encodeVarUInt 1
  encodeBool is_overflows -- not overflows
  encodeVarUInt 2

  if bucket_num == 0
    then encodeVarInt32 (-1)
    else encodeVarInt32 bucket_num
  encodeVarUInt 0