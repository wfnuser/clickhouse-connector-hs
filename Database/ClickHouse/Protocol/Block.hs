{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Block where

import Data.Int
import qualified Database.ClickHouse.Protocol.ClientProtocol as CP
import Database.ClickHouse.Protocol.Column
import Database.ClickHouse.Protocol.Const
import Database.ClickHouse.Protocol.Decoder
import Database.ClickHouse.Protocol.Encoder
import Database.ClickHouse.Protocol.Packet
import qualified Database.ClickHouse.Protocol.QueryProcessingStage as Stage
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
import Debug.Trace
import Z.Data.ASCII
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import qualified Z.Data.Vector as V

data BlockInfo = BlockInfo
  { is_overflows :: !Bool,
    bucket_num :: {-# UNPACK #-} !Int32
  }
  deriving (Show)

defaultBlockInfo :: BlockInfo
defaultBlockInfo = BlockInfo False 0

data Block = ColumnOrientedBlock
  { columns_with_type :: V.Vector (V.Bytes, V.Bytes),
    blockdata :: V.Vector (V.Vector CKType)
  }
  deriving (Show)

-- TODO: Provide a method to Consturct Block from rows

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
    Just (ColumnOrientedBlock cwt blockdata) -> do
      encodeVarUInt . fromIntegral . V.length $ blockdata
      encodeVarUInt . fromIntegral . V.length $ V.index blockdata 0
      loop 0 (ColumnOrientedBlock cwt blockdata)
  where
    loop i (ColumnOrientedBlock cwt bd) = do
      if i == V.length cwt
        then blockBuilder "" Nothing
        else do
          let (cn, ct) = V.index cwt i
          let rows = V.index bd i
          encodeBinaryStr cn
          encodeBinaryStr ct
          encodeCK rows
          loop (i + 1) (ColumnOrientedBlock cwt bd)

blockInfoBuilder :: BlockInfo -> B.Builder ()
blockInfoBuilder (BlockInfo is_overflows bucket_num) = do
  encodeVarUInt 1
  encodeBool is_overflows -- not overflows
  encodeVarUInt 2

  if bucket_num == 0
    then encodeVarInt32 (-1)
    else encodeVarInt32 bucket_num
  encodeVarUInt 0

blockInfoParser :: P.Parser BlockInfo
blockInfoParser = do
  num1 <- decodeVarUInt
  is_overflows <- decodeBool
  num2 <- decodeVarUInt
  bucket_num <- decodeVarInt32
  num3 <- decodeVarUInt
  return $ BlockInfo is_overflows bucket_num

blockParser :: P.Parser (Block, BlockInfo)
blockParser = do
  tablename <- decodeBinaryStr
  -- trace ("trace tablename: " ++ (show . T.validate $ tablename)) return ()
  info <- blockInfoParser
  cols <- decodeVarUInt
  rows <- decodeVarUInt
  -- trace ("trace rows: " ++ show rows) return ()
  block <- loop cols rows (ColumnOrientedBlock V.empty V.empty)
  return (block, info)
  where
    -- blockdata :: V.Vector CKType
    loopRow :: Word -> V.Bytes -> V.Vector CKType -> P.Parser (V.Vector CKType)
    loopRow rows colType blockdata
      | rows == 0 = return blockdata
      | otherwise = do
        val <- decodeCK colType
        let blockdata' = V.cons val blockdata
        loopRow (rows -1) colType blockdata'
    -- block :: Block
    loop :: Word -> Word -> Block -> P.Parser Block
    loop cols rows block
      | cols == 0 = return block
      | otherwise = do
        colName <- decodeBinaryStr
        colType <- decodeBinaryStr
        let block' = block {columns_with_type = V.cons (colName, colType) (columns_with_type block)}
        blockcol <- loopRow rows colType V.empty
        let block'' = block' {blockdata = V.cons blockcol $ blockdata block'}
        loop (cols -1) rows block''

-- [1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0,1,0,1,0,2,255,255,255,255,0,2,2,1,120,6,83,116,114,105,110,103,5,120,115,97,100,102,3,120,120,120,1,121,5,73,110,116,49,54,4,0,123,0]