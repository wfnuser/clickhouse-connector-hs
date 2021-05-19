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

loopDecodeNum :: (Num a, Bits a) => Int -> Int -> a -> P.Parser a
loopDecodeNum bit n ans =
  if n == bit
    then return ans
    else do
      byte <- P.satisfy $ const True
      let ans' = ans .|. ((fromIntegral byte .&. 0xFF) `unsafeShiftL` (8 * n))
      loopDecodeNum bit (n + 1) ans'

decodeVarInt32 :: P.Parser Int32
decodeVarInt32 = loopDecodeNum 4 0 0

decodeVarInt16 :: P.Parser Int16
decodeVarInt16 = loopDecodeNum 2 0 0

decodeBinaryStr :: P.Parser V.Bytes
decodeBinaryStr = do
  len <- decodeVarUInt
  P.take . fromIntegral $ len