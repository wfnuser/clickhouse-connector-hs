module Main where

import Database.ClickHouse.Connection

main :: IO ()
main = do
  conn <- connect defaultConnectInfo
  
  return ()