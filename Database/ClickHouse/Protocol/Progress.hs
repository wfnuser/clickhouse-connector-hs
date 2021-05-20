module Database.ClickHouse.Protocol.Progress where

import Data.Int
import Database.ClickHouse.Protocol.Decoder
import qualified Z.Data.Parser as P

data Progress = Progress
  { rows :: Int64,
    bytes :: Int64,
    totalRows :: Int64
  }

progressParser :: P.Parser Progress
progressParser = do
  rows <- decodeVarInt64
  bytes <- decodeVarInt64
  Progress rows bytes <$> decodeVarInt64
