{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Decoder where

import Control.Monad
import Data.Bits
import Data.Int
import Z.Data.ASCII
import qualified Z.Data.Parser as P
import qualified Z.Data.Vector as V

decodeVarUInt :: P.Parser Word
decodeVarUInt = loop 0 0
  where
    loop :: Int -> Word -> P.Parser Word
    loop i x =
      if i >= 9
        then return x
        else do
          byte <- P.satisfy $ const True
          let x' = x .|. ((fromIntegral byte .&. 0x7F) `unsafeShiftL` (7 * i))
          if (byte .&. 0x80) > 0
            then loop (i + 1) x'
            else return x'

decodeBool :: P.Parser Bool
decodeBool = do
  num <- decodeVarUInt
  if num == 1
    then return True
    else return False

-- TODO: define a loop helper function
decodeVarInt32 :: P.Parser Int32
decodeVarInt32 = loop 0 0
  where
    loop :: Int -> Int32 -> P.Parser Int32
    loop n ans =
      if n == 4
        then return ans
        else do
          byte <- P.satisfy $ const True
          let ans' = ans .|. ((fromIntegral byte .&. 0xFF) `unsafeShiftL` (8 * n))
          loop (n + 1) ans'

decodeVarInt16 :: P.Parser Int32
decodeVarInt16 = loop 0 0
  where
    loop :: Int -> Int32 -> P.Parser Int32
    loop n ans =
      if n == 2
        then return ans
        else do
          byte <- P.satisfy $ const True
          let ans' = ans .|. ((fromIntegral byte .&. 0xFF) `unsafeShiftL` (8 * n))
          loop (n + 1) ans'

decodeBinaryStr :: P.Parser V.Bytes
decodeBinaryStr = do
  len <- decodeVarUInt
  P.take . fromIntegral $ len