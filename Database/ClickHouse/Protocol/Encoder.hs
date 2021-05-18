{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Encoder where

import Control.Monad
import Data.Bits
import Data.Int
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

-- encodeLoop :: Bits a => Int -> a -> B.Builder ()
-- encodeLoop n acc = 
--       if n == 0
--         then return ()
--         else do
--           B.word8 (fromIntegral (acc .&. 0xFF))
--           let acc' = acc `unsafeShiftR` 8
--           encodeLoop (n -1) acc'

encodeVarInt32 :: Int32 -> B.Builder ()
encodeVarInt32 = loop 4
  where
    loop n acc =
      if n == 0
        then return ()
        else do
          B.word8 (fromIntegral (acc .&. 0xFF))
          let acc' = acc `unsafeShiftR` 8
          loop (n -1) acc'

encodeVarInt16 :: Int16 -> B.Builder ()
encodeVarInt16 = loop 2
  where
    loop n acc =
      if n == 0
        then return ()
        else do
          B.word8 (fromIntegral (acc .&. 0xFF))
          let acc' = acc `unsafeShiftR` 8
          loop (n -1) acc'

encodeBinaryStr :: V.Bytes -> B.Builder ()
encodeBinaryStr str = do
  let l = V.length str
  encodeVarUInt . fromIntegral $ l
  B.bytes str
