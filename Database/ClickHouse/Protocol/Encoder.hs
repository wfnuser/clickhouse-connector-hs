{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Encoder where

import Control.Monad
import Data.Bits
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

encodeBinaryStr :: V.Bytes -> B.Builder ()
encodeBinaryStr str = do
  let l = V.length str
  encodeVarUInt . fromIntegral $ l
  B.bytes str
