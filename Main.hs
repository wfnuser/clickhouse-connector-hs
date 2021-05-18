{-# LANGUAGE OverloadedStrings #-}

module Main where

import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Connect
import Database.ClickHouse.Protocol.Insert
import Database.ClickHouse.Protocol.Query

main :: IO ()
main = do
  conn <- connect defaultConnectInfo exec
  return ()
  where
    exec = do
      -- sendQuery "insert into big (*) VALUES ('a')"
      -- sendQuery "insert into big (*) VALUES ('b')"
      -- sendQuery "drop table if exists big"
      -- sendQuery "CREATE TABLE big (x String) ENGINE = Memory AS SELECT 1"
      prepareInsert "big" "INSERT INTO big (x) VALUES "