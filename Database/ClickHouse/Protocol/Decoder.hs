{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Decoder where

import Control.Monad
import Data.Bits
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
            then return x'
            else loop (i + 1) x'

decodeBinaryStr :: P.Parser V.Bytes
decodeBinaryStr = do
  len <- decodeVarUInt
  P.take . fromIntegral $ len