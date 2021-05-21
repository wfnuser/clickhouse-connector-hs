{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Block
import Database.ClickHouse.Protocol.Column
import Database.ClickHouse.Protocol.Connect (withConnect)
import Database.ClickHouse.Protocol.Insert
import Database.ClickHouse.Protocol.Meta
import Database.ClickHouse.Protocol.Query
import qualified Z.Data.Vector as V

main :: IO ()
main = withConnect defaultConnectInfo \info conn ->
  do
    -- insert part
    prepareInsert "test" "INSERT INTO test (x, y) VALUES " info conn
    let columns_with_type = [("x", "String"), ("y", "Int16")]
    let block =
          ColumnOrientedBlock
            { columns_with_type = V.pack columns_with_type,
              blockdata = V.pack [V.pack [CKString "xsadf", CKString "xxx", CKString "test"], V.pack [CKInt16 4, CKInt16 123, CKInt16 777]]
            }
    sendData "test" block conn
    meta <- readMeta conn

    -- select part
    sendQuery "select * from test limit 3" "test" conn

    meta <- readMeta conn
    case meta of
      Left error -> print error
      Right (MetaData (block, info)) -> do
        let cols = V.length $ blockdata block
        if cols == 0
          then print "empty"
          else do
            print $ show block