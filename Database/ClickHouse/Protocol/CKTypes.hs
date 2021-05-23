module Database.ClickHouse.Protocol.CKTypes where


import Data.Int
import qualified Z.Data.Vector as V

data CKType
  = CKInt16 Int16
  | CKInt32 Int32 
  | CKInt64 Int64
  | CKString V.Bytes
  | CKArray (V.Vector CKType)
  deriving (Show)
