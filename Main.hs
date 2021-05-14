{-# LANGUAGE OverloadedStrings #-}

module Main where

import Database.ClickHouse.Connection
import Database.ClickHouse.Protocol.Query

main :: IO ()
main = do
  conn <- connect defaultConnectInfo exec
  return ()
  where
    exec = do
      sendQuery "drop table if exists big"