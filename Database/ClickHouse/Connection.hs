{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Connection where

import Data.Char
import Data.IORef
import Database.ClickHouse.DNS
import Database.ClickHouse.Protocol.Block
import Database.ClickHouse.Protocol.Packet
import Z.Data.ASCII
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import qualified Z.Data.Vector as V
import Z.IO
import Z.IO.Network

data CHConn = CHConn
  { chRead :: {-# UNPACK #-} !(IO V.Bytes),
    chWrite :: V.Bytes -> IO (),
    isConsumed :: {-# UNPACK #-} !(IORef Bool)
  }

data ConnectInfo = ConnectInfo
  { ciHost :: HostName,
    ciPort :: PortNumber,
    ciDatabase :: V.Bytes,
    ciUser :: V.Bytes,
    ciPassword :: V.Bytes
  }
  deriving (Show)

defaultConnectInfo :: ConnectInfo
defaultConnectInfo = ConnectInfo "127.0.0.1" 9000 "" "default" ""