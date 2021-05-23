{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Encoder where

import Control.Monad
import Data.Bits
import Data.Int
import Database.ClickHouse.Protocol.CKTypes
import Debug.Trace
import Z.Data.ASCII
import qualified Z.Data.Builder as B
import qualified Z.Data.Vector as V

encodeVarUInt :: Word -> B.Builder ()
encodeVarUInt x = do
  let byte = x .&. 0x7F
  if x > 0x7F
    then B.word8 (fromIntegral (byte .|. 0x80))
    else B.word8 (fromIntegral byte)
  let x' = x `unsafeShiftR` 7
  when (x' /= 0) (encodeVarUInt x')

encodeBool :: Bool -> B.Builder ()
encodeBool is_overflows = if is_overflows then encodeVarUInt 1 else encodeVarUInt 0

encodeLoop :: (Bits a, Integral a) => Int -> a -> B.Builder ()
encodeLoop n acc =
  if n == 0
    then return ()
    else do
      B.word8 (fromIntegral (acc .&. 0xFF))
      let acc' = acc `unsafeShiftR` 8
      encodeLoop (n -1) acc'

encodeVarInt64 :: Int64 -> B.Builder ()
encodeVarInt64 = encodeLoop 8

encodeVarInt32 :: Int32 -> B.Builder ()
encodeVarInt32 = encodeLoop 4

encodeVarInt16 :: Int16 -> B.Builder ()
encodeVarInt16 = encodeLoop 2

encodeBinaryStr :: V.Bytes -> B.Builder ()
encodeBinaryStr str = do
  let l = V.length str
  encodeVarUInt . fromIntegral $ l
  B.bytes str


-- V.pack [CKArray (V.pack [CKArray V.empty,CKArray V.empty,CKArray V.empty]),CKArray (V.pack [CKArray V.empty])]