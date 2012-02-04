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
  , createSource
  , FTPError(..)
  ) where

import Data.Conduit
import qualified Data.ByteString as BS
import Data.ByteString.UTF8 hiding (foldl)
import Network.Socket hiding (send, sendTo, recv, recvFrom, Closed)
import Network.Socket.ByteString
import Network.URI
import Network.Utils
import Control.Monad.Error
import Data.Word
import System.ByteOrder
import Data.Bits
import Prelude hiding (getLine)
import Control.Monad.Trans.Resource

data FTPError = UnexpectedCode Int BS.ByteString
                | GeneralError String
                | IncorrectScheme String
                | SocketClosed
  deriving (Show)
instance Error FTPError where
  noMsg  = GeneralError ""
  strMsg = GeneralError

hton_16 :: Word16 -> Word16
hton_16 x = case byteOrder of
  BigEndian -> x
  LittleEndian -> x `shiftL` 8 + x `shiftR` 8
  _ -> undefined

getByte :: Socket -> ResourceT (ErrorT FTPError IO) Word8
getByte s = do
  b <- lift $ lift $ recv s 1
  if BS.null b then lift (throwError SocketClosed) else return $ BS.head b

getLine :: Socket -> ResourceT (ErrorT FTPError IO) BS.ByteString
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

readExpected :: Socket -> Int -> ResourceT (ErrorT FTPError IO) BS.ByteString
readExpected s i = do
  line <- getLine s
  --lift $ lift $ putStrLn $ "Read: " ++ (toString line)
  if extractCode line /= i
    then lift $ throwError $ UnexpectedCode i line
    else return line

writeLine :: Socket -> BS.ByteString -> ResourceT (ErrorT FTPError IO) ()
writeLine s bs = lift $ lift $ do
  --lift $ lift $ putStrLn $ "Writing: " ++ (toString bs)
  sendAll s $ bs `BS.append` (fromString "\r\n") -- hardcode the newline for platform independence

createSource :: URI -> Source (ErrorT FTPError IO) BS.ByteString
createSource uri = Source { sourcePull = pull
                           , sourceClose = close
                           }

  where pull = do
          (c, rc, d, rd, path') <- common uri
          writeLine c $ fromString $ "RETR " ++ path'
          _ <- readExpected c 150
          pull' c rc d rd
        pull' c rc d rd= do
          bytes <- lift $ lift $ recv d 1024
          if BS.null bytes
            then do
              close' c rc d rd
              return Closed
            else do
              return $ Open (Source { sourcePull = pull' c rc d rd
                                    , sourceClose = close' c rc d rd
                                    }) bytes
        close = return ()
        close' c rc _ rd = do
          release rd
          _ <- readExpected c 226
          writeLine c $ fromString "QUIT"
          _ <- readExpected c 221
          release rc

createSink :: URI -> Sink BS.ByteString (ErrorT FTPError IO) ()
createSink uri = SinkData { sinkPush = push
                           , sinkClose = close
                           }
  where push input = do
          (c, rc, d, rd, path') <- common uri
          writeLine c $ fromString $ "STOR " ++ path'
          _ <- readExpected c 150
          push' c rc d rd input
        push' c rc d rd input = do
          lift $ lift $ sendAll d input
          return $ Processing (push' c rc d rd) (close' c rc d rd)
        close = return ()
        close' c rc _ rd = do
          release rd
          _ <- readExpected c 226
          writeLine c $ fromString "QUIT"
          _ <- readExpected c 221
          release rc

common :: URI -> ResourceT (ErrorT FTPError IO) (Socket, ReleaseKey, Socket, ReleaseKey, String)
common (URI { uriScheme = scheme'
       , uriAuthority = authority'
       , uriPath = path'
       }) = do
  if scheme' /= "ftp:" then lift (throwError (IncorrectScheme scheme')) else return ()
  c <- lift $ lift $ connectTCP host (PortNum (hton_16 port))
  rc <- register $ sClose c
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
  d <- lift $ lift $ connectTCP (toString pasvhost) (PortNum (hton_16 pasvport))
  rd <- register $ sClose d
  return (c, rc, d, rd, path')
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
