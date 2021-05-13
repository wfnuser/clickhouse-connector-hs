{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Packet where

import Database.ClickHouse.Protocol.Const
import Database.ClickHouse.Protocol.Encoder
import Z.Data.ASCII
import qualified Z.Data.Builder as B
import qualified Z.Data.Vector as V

data Hello = Hello {db :: V.Bytes, username :: V.Bytes, password :: V.Bytes} deriving (Show)

helloBuilder :: Hello -> B.Builder ()
helloBuilder (Hello db username password) = do
  encodeVarUInt _HELLO
  encodeBinaryStr ("ClickHouse" <> _CLIENT_NAME)
  encodeVarUInt _CLIENT_VERSION_MAJOR
  encodeVarUInt _CLIENT_VERSION_MINOR
  encodeVarUInt _CLIENT_REVISION
  encodeBinaryStr db
  encodeBinaryStr username
  encodeBinaryStr password

test :: IO ()
test = print . B.build . helloBuilder $ h
  where
    h = Hello "big" "root" "123456"
