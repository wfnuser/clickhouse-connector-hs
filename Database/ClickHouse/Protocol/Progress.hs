module Database.ClickHouse.Protocol.Progress where

import Data.Int
import Database.ClickHouse.Protocol.Decoder
import qualified Z.Data.Parser as P

data Progress = Progress
  { rows :: Word,
    bytes :: Word,
    totalRows :: Word,
    writtenRows :: Word,
    writtenBytes :: Word
  }
  deriving (Show)

progressParser :: P.Parser Progress
progressParser = do
  rows <- decodeVarUInt
  bytes <- decodeVarUInt
  totalRows <- decodeVarUInt
  writtenRows <- decodeVarUInt
  Progress rows bytes totalRows writtenRows <$> decodeVarUInt
