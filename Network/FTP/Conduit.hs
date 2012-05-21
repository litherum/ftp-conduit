{-# LANGUAGE DeriveDataTypeable #-}
-- | This module contains code to use files on a remote FTP server as
-- Sources and Sinks.
--
-- Using these functions looks like this:
-- > let uri = fromJust $ parseURI "ftp://ftp.kernel.org/pub/README_ABOUT_BZ2_FILES"
-- > runErrorT $ runResourceT $ createSource uri $$ consume
--
-- The functions here operate on the ErrorT monad transformer, because
-- the server can send unexpected replies, which are thrown as errors.
module Network.FTP.Conduit
  ( createSink
--  , createSource
  , FTPException(..)
  ) where

import Data.Conduit
import qualified Data.ByteString as BS
import Data.ByteString.UTF8 hiding (foldl)
import Network.Socket hiding (send, sendTo, recv, recvFrom, Closed)
import Network.Socket.ByteString
import Network.URI
import Network.Utils
import Control.Exception
import Data.Word
import System.ByteOrder
import Data.Bits
import Prelude hiding (getLine)
import Control.Monad.Trans
import Data.Typeable

data FTPException = UnexpectedCode Int BS.ByteString
                  | GeneralError String
                  | IncorrectScheme String
                  | SocketClosed
  deriving (Typeable, Show)

instance Exception FTPException

hton_16 :: Word16 -> Word16
hton_16 x = case byteOrder of
  BigEndian -> x
  LittleEndian -> x `shiftL` 8 + x `shiftR` 8
  _ -> undefined

getByte :: Socket -> IO Word8
getByte s = do
  b <- recv s 1
  if BS.null b then throw SocketClosed else return $ BS.head b

getLine :: Socket -> IO BS.ByteString
getLine s = do
  b <- getByte s
  helper b
  where helper b = do
          b' <- getByte s
          if BS.pack [b, b'] == fromString "\r\n"
            then return BS.empty
            else helper b' >>= return . (BS.cons b)

extractCode :: BS.ByteString -> Int
extractCode = read . toString . (BS.takeWhile (/= 32))

readExpected :: Socket -> Int -> IO BS.ByteString
readExpected s i = do
  line <- getLine s
  --putStrLn $ "Read: " ++ (toString line)
  if extractCode line /= i
    then throw $ UnexpectedCode i line
    else return line

writeLine :: Socket -> BS.ByteString -> IO ()
writeLine s bs = do
  --putStrLn $ "Writing: " ++ (toString bs)
  sendAll s $ bs `BS.append` (fromString "\r\n") -- hardcode the newline for platform independence

createSource :: MonadResource m => URI -> Source m BS.ByteString
createSource uri = sourceIO setup close pull
  where setup = do
          (c, d, path') <- common uri
          writeLine c $ fromString $ "RETR " ++ path'
          _ <- readExpected c 150
          return (c, d)
        close (c, d) = do
          sClose d
          _ <- readExpected c 226
          writeLine c $ fromString "QUIT"
          _ <- readExpected c 221
          sClose c
        pull (c, d) = liftIO $ do
          bytes <- recv d 1024
          if BS.null bytes
            then do
              close (c, d)
              return IOClosed
            else return $ IOOpen bytes

createSink :: MonadResource m => URI -> Sink BS.ByteString m ()
createSink uri = sinkIO setup close push (liftIO . close)
  where setup = do
          (c, d, path') <- common uri
          writeLine c $ fromString $ "STOR " ++ path'
          _ <- readExpected c 150
          return (c, d)
        close (c, d) = do
          sClose d
          _ <- readExpected c 226
          writeLine c $ fromString "QUIT"
          _ <- readExpected c 221
          sClose c
        push (c, d) input = liftIO $ do
          sendAll d input
          return IOProcessing

common :: URI -> IO (Socket, Socket, String)
common (URI { uriScheme = scheme'
       , uriAuthority = authority'
       , uriPath = path'
       }) = liftIO $ do
  if scheme' /= "ftp:" then throw $ IncorrectScheme scheme' else return ()
  c <- connectTCP host (PortNum (hton_16 port))
  _ <- readExpected c 220
  writeLine c $ fromString $ "USER " ++ user
  _ <- readExpected c 331
  writeLine c $ fromString $ "PASS " ++ pass
  _ <- readExpected c 230
  writeLine c $ fromString "TYPE I"
  _ <- readExpected c 200
  writeLine c $ fromString "PASV"
  pasv_response <- readExpected c 227
  let (pasvhost, pasvport) = parsePasvString pasv_response
  d <- connectTCP (toString pasvhost) (PortNum (hton_16 pasvport))
  return (c, d, path')
  where (host, port, user, pass) = case authority' of
          Nothing -> undefined
          Just (URIAuth userInfo regName port') ->
            ( regName
            , if null port' then 21 else read (tail port')
            , if null userInfo then "anonymous" else takeWhile (\ l -> l /= ':' && l /= '@') userInfo
            , if null userInfo || not (':' `elem` userInfo) then "" else init $ tail $ (dropWhile (/= ':')) userInfo
            )
        parsePasvString ps = (pasvhost, pasvport)
          where pasvhost = BS.init $ foldl (\ a ip -> a `BS.append` (fromString $ show ip) `BS.append` (fromString ".")) BS.empty [ip1, ip2, ip3, ip4]
                pasvport = (fromIntegral port1) `shiftL` 8 + (fromIntegral port2)
                (ip1, ip2, ip3, ip4, port1, port2) = read $ toString $ (`BS.append` (fromString ")")) $ (BS.takeWhile (/= 41)) $ (BS.dropWhile (/= 40)) ps :: (Int, Int, Int, Int, Int, Int)
