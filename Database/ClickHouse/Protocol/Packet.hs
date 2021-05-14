{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Packet where

import qualified Database.ClickHouse.Protocol.ClientProtocol as CP
import Database.ClickHouse.Protocol.Const
import Database.ClickHouse.Protocol.Decoder
import Database.ClickHouse.Protocol.Encoder
import qualified Database.ClickHouse.Protocol.ServerProtocol as SP
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
    versionMajor :: {-# UNPACK #-} !Word,
    versionMinor :: {-# UNPACK #-} !Word,
    versionPatch :: {-# UNPACK #-} !Word,
    revision :: !Word,
    timezone :: Maybe V.Bytes,
    displayName :: {-# UNPACK #-} !V.Bytes
  }
  deriving (Show)

serverInfoParser :: P.Parser ServerInfo
serverInfoParser = do
  packetType <- decodeVarUInt
  if packetType == SP._HELLO
    then do
      serverName <- decodeBinaryStr
      serverVersionMajor <- decodeVarUInt
      serverVersionMinor <- decodeVarUInt
      revision <- decodeVarUInt
      timezone <-
        if revision >= _DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
          then do Just <$> decodeBinaryStr
          else return Nothing
      displayName <-
        if revision >= _DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
          then decodeBinaryStr
          else return ""
      versionPatch <-
        if revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH
          then decodeVarUInt
          else return revision
      return $
        ServerInfo
          { name = serverName,
            versionMajor = serverVersionMajor,
            versionMinor = serverVersionMinor,
            versionPatch = versionPatch,
            revision = revision,
            timezone = timezone,
            displayName = displayName
          }
    else P.fail' "parse failed"
