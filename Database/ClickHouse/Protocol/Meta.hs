{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Meta where

import Control.Monad
import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Block
import Database.ClickHouse.Protocol.Column
import Database.ClickHouse.Protocol.Decoder
import qualified Database.ClickHouse.Protocol.ProfileInfo as PI
import qualified Database.ClickHouse.Protocol.Progress as PG
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
import Debug.Trace
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import Z.Data.Vector as V

-- data MetaInfo = Block
-- TODO: metainfo should support more than block

data MetaInfo = MetaData (Block, BlockInfo) | MetaException V.Bytes | MetaProgress PG.Progress | MetaProfileInfo PI.ProfileInfo | MetaEndOfStream
  deriving (Show)

metaParser :: P.Parser MetaInfo
metaParser = do
  packetType <- decodeVarUInt
  case packetType of
    packetType
      | packetType == SP._DATA -> MetaData <$> blockParser
      -- TODO: support the internal error
      | packetType == SP._EXCEPTION -> return $ MetaException "internal error"
      | packetType == SP._PROGRESS -> MetaProgress <$> PG.progressParser
      | packetType == SP._PROFILE_INFO -> MetaProfileInfo <$> PI.profileInfoParser
      | packetType == SP._END_OF_STREAM -> return MetaEndOfStream
      | otherwise -> return $ MetaException "unknown types"

readMeta :: CHConn -> IO (Either P.ParseError MetaInfo)
readMeta c = do
  metas <- genMetas "" []
  let blocks = Prelude.filter isBlock metas
  return $ Right $ mergeBlocks blocks
  where
    mergeBlocks :: [MetaInfo] -> MetaInfo
    mergeBlocks (x : xs) = do
      let (MetaData (block, info)) = x
      let bd = blockdata block
      if V.length bd == 0
        then mergeBlocks xs
        else do
          let (MetaData (nextblock, nextinfo)) = mergeBlocks xs
          let nextbd = blockdata nextblock
          let combinedBlockData = if V.length nextbd == 0 then bd else V.zipWith' V.append bd nextbd

          let combinedBlock =
                ColumnOrientedBlock
                  { blockdata = combinedBlockData,
                    columns_with_type = if columns_with_type block == V.empty then columns_with_type nextblock else columns_with_type block
                  }
          MetaData (combinedBlock, info)
    mergeBlocks [] = MetaData (ColumnOrientedBlock V.empty V.empty, defaultBlockInfo)

    isBlock :: MetaInfo -> Bool
    isBlock metaInfo = case metaInfo of
      MetaData m -> True
      _ -> False

    genMetas :: Bytes -> [MetaInfo] -> IO [MetaInfo]
    genMetas buf acc = do
      if buf == ""
        then do
          buf <- chRead c
          genMetas buf acc
        else do
          let (remain, meta) = P.parse metaParser buf
          case meta of
            Left error -> return []
            Right MetaEndOfStream -> return acc
            Right (MetaProgress progress) -> do
              print "show progress..."
              print $ "current rows: " ++ show (PG.rows progress)
              print $ "current bytes: " ++ show (PG.bytes progress)
              print $ "total rows: " ++ show (PG.totalRows progress)
              genMetas remain acc
            Right meta -> genMetas remain (meta : acc)