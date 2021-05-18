{-# LANGUAGE FlexibleContexts #-}

module Database.ClickHouse.Protocol.Column where

import Data.Int
import Database.ClickHouse.Protocol.Encoder
import qualified Z.Data.Builder as B
import qualified Z.Data.Vector as V

data CKType
  = CKInt16 Int16
  | CKString V.Bytes
  deriving (Show)

encodeCK :: V.Vector CKType -> B.Builder ()
encodeCK cks = loop 0 cks
  where
    loop i cks = do
      if i == V.length cks
        then return ()
        else do
          let ck = V.index cks i
          case ck of
            CKInt16 v -> do
              encodeVarInt16 v
            CKString v -> do
              encodeBinaryStr v
          loop (i + 1) cks