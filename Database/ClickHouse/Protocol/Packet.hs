{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Packet where

import qualified Database.ClickHouse.Protocol.ClientProtocol as CP
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
import Database.ClickHouse.Protocol.Const
import Database.ClickHouse.Protocol.Encoder
import Database.ClickHouse.Protocol.Decoder
import Z.Data.ASCII
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P
import qualified Z.Data.Vector as V

data Hello = Hello {db :: V.Bytes, username :: V.Bytes, password :: V.Bytes} deriving (Show)

helloBuilder :: Hello -> B.Builder ()
helloBuilder (Hello db username password) = do
  encodeVarUInt CP._HELLO
  encodeBinaryStr ("ClickHouse" <> _CLIENT_NAME)
  encodeVarUInt _CLIENT_VERSION_MAJOR
  encodeVarUInt _CLIENT_VERSION_MINOR
  encodeVarUInt _CLIENT_REVISION
  encodeBinaryStr db
  encodeBinaryStr username
  encodeBinaryStr password

data ServerInfo = ServerInfo
  { name :: {-# UNPACK #-} !V.Bytes,
    version_major :: {-# UNPACK #-} !Word,
    version_minor :: {-# UNPACK #-} !Word,
    version_patch :: {-# UNPACK #-} !Word,
    revision :: !Word,
    timezone :: Maybe V.Bytes,
    display_name :: {-# UNPACK #-} !V.Bytes
  }
  deriving (Show)

-- serverInfoParser :: V.Bytes -> P.Parser ServerInfo
-- serverInfoParser bytes = do
--   packet_type <- decodeVarUInt
--   case packet_type of 
--     SP._HELLO ->

--     otherwise ->
  
