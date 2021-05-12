{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Connection where
import Data.IORef
import qualified Z.Data.Vector as V
import Z.IO
import Database.ClickHouse.DNS
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
defaultConnectInfo = ConnectInfo "127.0.0.1" 9000 "" "root" ""

connect :: ConnectInfo -> IO CHConn
connect (ConnectInfo host port db user pass) = do
  addr <- resolveDNS (host, port)
  withResource (initTCPClient defaultTCPClientConfig {tcpRemoteAddr = addrAddress addr}) $ \tcp -> do
    (i, o) <- newBufferedIO tcp
    consumed <- newIORef True
    let chConn = CHConn (readBuffer i) (writeBuffer o) consumed
    return chConn
