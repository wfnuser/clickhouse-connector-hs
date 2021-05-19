{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Meta where

import Control.Monad
import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Block
import Database.ClickHouse.Protocol.Decoder
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import Z.Data.Vector as V

-- data MetaInfo = Block
-- TODO: metainfo should support more than block

metaParser :: P.Parser (Block, BlockInfo)
metaParser = do
  packetType <- decodeVarUInt
  blockParser

readMeta :: CHConn -> IO ()
readMeta c = do
  buf <- chRead c
  print buf
  let res = P.parse metaParser buf
  case res of
    (remain, Left err) -> do
      print err
    (remain, Right (block, info)) -> do
      print $ is_overflows info
      print $ bucket_num info
      print $ V.length (columns_with_type block)
      mapM_
        (\(cn, ct) -> do print $ T.validate cn; print $ T.validate ct)
        (V.unpack $ columns_with_type block)
      mapM_
        (mapM_ print)
        (V.unpack $ blockdata block)