{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Column where

import Data.Int
import Database.ClickHouse.Protocol.CKTypes
import Database.ClickHouse.Protocol.Decoder
import Database.ClickHouse.Protocol.Encoder
import Debug.Trace
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P
import qualified Z.Data.Vector as V

-- encode Rows of CKType
encodeCKs :: V.Vector CKType -> B.Builder ()
encodeCKs cks =
  if V.length cks == 0
    then return ()
    else case V.head cks of
      CKArray arr -> do
        encodeArray cks
      _ -> do
        loop 0 (trace ("encodeCKs rows: " ++ show cks) cks)
        where
          loop i cks = do
            if i == V.length cks
              then return ()
              else do
                let ck = V.index cks i
                encodeCK ck
                loop (i + 1) cks

encodeCK :: CKType -> B.Builder ()
encodeCK ck = case ck of
  CKInt16 v -> do
    encodeVarInt16 v
  CKString v -> do
    encodeBinaryStr v

-- CKArray v -> do
--   encodeArray v

decodeCK :: V.Bytes -> P.Parser CKType
decodeCK t = do
  case t of
    "Int16" -> CKInt16 <$> decodeVarInt16
    "String" -> CKString <$> decodeBinaryStr

-- encodeCKs :: CKType ->

-- [[[1,2],[3]],[[5],[6,7,8]]]
-- [2] [1, 2] [2,3,4,7]

-- Encode An Array
encodeArray :: V.Vector CKType -> B.Builder ()
encodeArray v = do
  trace (show v) return ()
  let (lengths, flattern) = computeArrayLen v

  -- trace (show lengths) (return ())
  mapM_ encodeVarInt64 (drop 1 lengths)
  mapM_ encodeCK flattern

-- return ()

-- (lengths, flattern typs)
computeArrayLen :: V.Vector CKType -> ([] Int64, V.Vector CKType)
computeArrayLen v = (fromIntegral (V.length v) : arrayLens, flattern)
  where
    (arrayLens, flattern) = loop v []
    getCKArray :: CKType -> V.Vector CKType
    getCKArray (CKArray v) = v
    getCKArrayLen :: CKType -> Int64
    getCKArrayLen (CKArray v) = fromIntegral $ V.length v
    loop :: V.Vector CKType -> [] Int64 -> ([] Int64, V.Vector CKType)
    loop v res = do
      case V.unpack v of
        ((CKArray x) : xs) -> do
          let lengths = map (fromIntegral . getCKArrayLen) (V.unpack v)
          let cumLengths = foldl (\acc each -> if null acc then [each] else acc ++ [each + last acc]) [] lengths
          let flattern = V.foldl' (\acc each -> V.append acc (getCKArray each)) V.empty v
          loop flattern (res ++ cumLengths)
        _ -> (res, v)