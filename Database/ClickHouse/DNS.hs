module Database.ClickHouse.DNS where

import qualified Z.Data.Builder as B
import Z.Data.CBytes
import Z.IO.Network

resolveDNS :: (HostName, PortNumber) -> IO AddrInfo
resolveDNS (hostName, portNumber) = head <$> getAddrInfo Nothing hostName (buildCBytes . B.int $ portNumber)