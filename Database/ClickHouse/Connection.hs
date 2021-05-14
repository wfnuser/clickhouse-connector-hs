{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Connection where

import Data.Char
import Data.IORef
import Database.ClickHouse.DNS
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

connect :: ConnectInfo -> (CHConn -> IO CHConn) -> IO CHConn
connect (ConnectInfo host port database username password) func = do
  addr <- resolveDNS (host, port)
  withResource (initTCPClient defaultTCPClientConfig {tcpRemoteAddr = addrAddress addr}) $ \tcp -> do
    (i, o) <- newBufferedIO tcp
    consumed <- newIORef True
    let hello = Hello database username password
    writeBuffer' o $ B.build . helloBuilder $ hello
    serverInfoBuf <- readBuffer i
    let (_, info) = P.parse serverInfoParser serverInfoBuf
    case info of
      Left error -> do
        print error
        -- should not use undefined to throw error
        undefined
      Right info -> do
        print . T.validate $ name info
        print . show $ versionMajor info
        print . show $ versionMinor info
        print . show $ versionPatch info
        print . show $ revision info
        case timezone info of
          Just timezone -> print . T.validate $ timezone
        print . T.validate $ displayName info
        let conn = CHConn (readBuffer i) (writeBuffer' o) consumed
        func conn
        return conn
