{-# LANGUAGE OverloadedStrings #-}

module Database.ClickHouse.Protocol.Connect where

import Data.IORef
import Database.ClickHouse.Connection
import Database.ClickHouse.DNS
import Database.ClickHouse.Protocol.Block
import Database.ClickHouse.Protocol.Column
import Database.ClickHouse.Protocol.Insert
import Database.ClickHouse.Protocol.Packet
import Database.ClickHouse.Protocol.Query
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P
import qualified Z.Data.Text as T
import qualified Z.Data.Vector as V
import Z.IO
import Z.IO.Network

connect :: ConnectInfo -> (ServerInfo -> CHConn -> IO CHConn) -> IO CHConn
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
        -- func info conn
        prepareInsert "test" "INSERT INTO test (x, y) VALUES " info conn

        let columns_with_type = [("x", "String"), ("y", "Int16")]
        let block =
              ColumnOrientedBlock
                { columns_with_type = V.pack columns_with_type,
                  blockdata = V.pack [V.pack [CKString "xsadf", CKString "xxx"], V.pack [CKInt16 4, CKInt16 123]]
                }
        sendData "test" block conn
        res <- readBuffer i

        sendQuery "select * from test" "test" conn
        res <- readBuffer i
        print res
        res <- readBuffer i
        print res

        return conn