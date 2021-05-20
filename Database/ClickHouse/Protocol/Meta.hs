{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Meta where

import Control.Monad
import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Block
import Database.ClickHouse.Protocol.Column
import Database.ClickHouse.Protocol.Decoder
import Database.ClickHouse.Protocol.Progress
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import Z.Data.Vector as V

-- data MetaInfo = Block
-- TODO: metainfo should support more than block

data MetaInfo = MetaData Block | MetaException V.Bytes | MetaProgress Progress

metaParser :: P.Parser MetaInfo
metaParser = do
  packetType <- decodeVarUInt
  case packetType of
    packetType
      | packetType == SP._DATA -> MetaData <$> blockParser
      -- TODO: support the internal error
      | packetType == SP._EXCEPTION -> return $ MetaException "internal error"
      | packetType == SP._PROGRESS -> MetaProgress <$> progressParser

readMeta :: CHConn -> IO (Either P.ParseError MetaInfo)
readMeta c = loopReadMeta
  where
    loopReadMeta = do
      buf <- chRead c
      let (remain, meta) = P.parse metaParser buf
      case meta of
        Right (MetaProgress progress) -> do
          print $ "current rows: " ++ show (rows progress)
          print $ "current bytes: " ++ show (bytes progress)
          print $ "total rows: " ++ show (totalRows progress)
          loopReadMeta
        _ -> return meta

-- case res of
--   (remain, Left err) -> do
--     print err
--   (remain, Right (block, info)) -> do
--     print $ is_overflows info
--     print $ bucket_num info
--     print $ V.length (columns_with_type block)
--     mapM_
--       (\(cn, ct) -> do print $ T.validate cn; print $ T.validate ct)
--       (V.unpack $ columns_with_type block)
--     mapM_
--       (mapM_ print)
--       (V.unpack $ blockdata block)