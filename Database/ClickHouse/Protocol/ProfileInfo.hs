module Database.ClickHouse.Protocol.ProfileInfo where

import Data.Int
import Database.ClickHouse.Protocol.Decoder
import qualified Z.Data.Parser as P

data ProfileInfo = ProfileInfo
  { rows :: Word,
    block :: Word,
    bytes :: Word,
    appliedLimit :: Bool,
    rowsBeforeLimit :: Word,
    calculatedRowsBeforeLimit :: Bool
  }
  deriving (Show)

profileInfoParser :: P.Parser ProfileInfo
profileInfoParser = do
  rows <- decodeVarUInt
  blocks <- decodeVarUInt
  bytes <- decodeVarUInt
  appliedLimit <- decodeBool
  rowsBeforeLimit <- decodeVarUInt
  calculatedRowsBeforeLimit <- decodeBool
  ProfileInfo rows blocks bytes appliedLimit rowsBeforeLimit <$> decodeBool