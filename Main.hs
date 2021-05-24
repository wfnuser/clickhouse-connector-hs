{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Block
import Database.ClickHouse.Protocol.CKTypes
import Database.ClickHouse.Protocol.Column
import Database.ClickHouse.Protocol.Connect (withConnect)
import Database.ClickHouse.Protocol.Insert
import Database.ClickHouse.Protocol.Meta
import Database.ClickHouse.Protocol.Query
import qualified Z.Data.Text as T
import qualified Z.Data.Vector as V

main :: IO ()
main = withConnect defaultConnectInfo \info conn ->
  do
    -- insert part

    -- prepareInsert "arr" "INSERT INTO arr2 (x) VALUES " info conn
    -- let columns_with_type = [("x", "Array(Array(String))")]
    -- let block =
    --       ColumnOrientedBlock
    --         { columns_with_type = V.pack columns_with_type,
    --           -- blockdata = V.pack [V.pack [CKArray (V.pack [CKString "tmp"])]]
    --           blockdata =
    --             V.pack
    --               [ V.pack -- each item for one row
    --                   [ CKArray
    --                       ( V.pack
    --                           [ CKArray
    --                               ( V.pack
    --                                   [ CKString "silvia",
    --                                     CKString "wfnuser"
    --                                   ]
    --                               ),
    --                             CKArray
    --                               ( V.pack
    --                                   [ CKString "test1",
    --                                     CKString "test11",
    --                                     CKString "test111"
    --                                   ]
    --                               )
    --                           ]
    --                       ),
    --                     CKArray
    --                       ( V.pack
    --                           [ CKArray
    --                               ( V.pack
    --                                   [ CKString "batchtest"
    --                                   ]
    --                               )
    --                           ]
    --                       )
    --                   ]
    --               ]
    --         }
    -- sendData "arr" block conn
    -- buf <- chRead conn
    -- print buf

    -- meta <- readMeta conn
    -- meta <- readMeta conn

    -- return ()

    -- select part
    sendQuery "select * from arr2" "arr2" conn
    -- buf <- chRead conn
    -- print buf
    -- buf <- chRead conn
    -- print buf
    -- buf <- chRead conn
    -- print buf
    meta <- readMeta conn
    case meta of
      Left error -> print error
      Right (MetaData (block, info)) -> do
        let cols = V.length $ blockdata block
        if cols == 0
          then print "empty"
          else do
            print $ show block

    -- buf <- chRead conn
    -- print buf
    -- buf <- chRead conn
    -- print buf

    -- meta <- readMeta conn
    -- case meta of
    --   Left error -> print error
    --   Right (MetaData (block, info)) -> do
    --     let cols = V.length $ blockdata block
    --     if cols == 0
    --       then print "empty"
    --       else do
    --         print $ show block