-- | This module contains code to use files on a remote FTP server as
-- Sources and Sinks.
--
-- This module makes use of the FTP 'STOR' and 'RETR' commands. According
-- to the FTP spec, these
-- commands are set up so that a server response is sent at the beginning
-- and end of the data transfers. The current Conduit library doesn't
-- allow the code that creates the Source / Sink of the data transfer to
-- run checks _after_ the data has been transferred. Because of this fact, this module exposes
-- two interfaces.
--
-- One interface is an interface that simply creates a Source / Sink and
-- does not check the server return code after the data transfer (because
-- it can't, as stated above). These two functions (putFTPFile and
-- getFTPFile) return a Sink and a Source, respectively.
--
-- Using these functions looks like:
--
-- > runErrorT $
-- >   runResourceT $
-- >     (getFTPFile $ fromJust $ parseURI
-- >       "ftp://ftp.kernel.org/pub/README_ABOUT_BZ2_FILES") >>=
-- >         (\ (_, s, _, _) -> s $$ consume)
--
-- The other interface is one that does check the error code after the
-- data transfer. In order to do this, however, these functions must
-- actually be the ones that apply the '$$' (connect) function, so they
-- can perform the checks afterword. These are the connectDownloadToSink
-- and connectSourceToUpload functions. You pass them a Sink and a Source,
-- respectively, and they will call the '$$' (connect) function, check that
-- the data transfer was successful, and then safely tear down the FTP
-- connection. 
--
-- Using these functions looks like:
--
-- > runErrorT $ runResourceT $ connectDownloadToSink
-- >   (fromJust $ parseURI
-- >     "ftp://ftp.kernel.org/pub/README_ABOUT_BZ2_FILES") consume
--
-- The functions here operate on the ErrorT monad transformer, because
-- the server can send unexpected replies, which are thrown as errors.
module Network.FTP.Conduit
  ( FTPError(..)
  , putFTPFile
  , getFTPFile
  , connectDownloadToSink
  , connectSourceToUpload
  ) where

import Control.Monad.Trans.Resource
import Data.Word
import Data.Conduit
import Data.Conduit.Binary (sourceHandle, sinkHandle)
import Data.Bits
import Data.Typeable
import Network.URI
import Control.Exception
import Data.ByteString (ByteString)
import Network.Socket hiding (connect)
import Network.Utils
import Control.Monad.Error
import System.IO
import System.ByteOrder

-- | 'UnexpectedCode' has the form (Expected code, full response string)
data FTPError = UnexpectedCode Int String
                | GeneralError String
  deriving (Show)
instance Error FTPError where
  noMsg  = GeneralError ""
  strMsg = GeneralError

hton_16 :: Word16 -> Word16
hton_16 x = case byteOrder of
  BigEndian -> x
  LittleEndian -> x `shiftL` 8 + x `shiftR` 8

extractCode :: String -> Int
extractCode = read . (takeWhile (/= ' '))

readExpected :: Handle -> Int -> ResourceT (ErrorT FTPError IO) String
readExpected h i = do
  line <- liftIO $ hGetLine h
  --liftIO $ putStrLn $ "Read: " ++ line
  if extractCode line /= i
    then lift $ throwError $ UnexpectedCode i line
    else return line

writeLine :: Handle -> String -> ResourceT (ErrorT FTPError IO) ()
writeLine h s = liftIO $ do
  --liftIO $ putStrLn $ "Writing: " ++ s
  hPutStr h $ s ++ "\r\n" -- hardcode the newline for platform independence
  hFlush h -- Buffering doesn't work right for some reason. Explicitly flush here.

-- | Give this a source and it uploads it to the given URI. This function calls '$$'.
connectSourceToUpload :: URI -> Source (ErrorT FTPError IO) ByteString -> ResourceT (ErrorT FTPError IO) ()
connectSourceToUpload uri source = do
  (handle, sink, release_control, release_data) <- putFTPFile uri
  out <- source $$ sink
  cleanUp release_control release_data handle
  return out

-- | Give this a Sink and it downloads the given URI to it. This function calls '$$'.
connectDownloadToSink :: URI -> Sink ByteString (ErrorT FTPError IO) b -> ResourceT (ErrorT FTPError IO) b
connectDownloadToSink uri sink = do
  (handle, source, release_control, release_data) <- getFTPFile uri
  out <- source $$ sink
  cleanUp release_control release_data handle
  return out

cleanUp :: ReleaseKey -> ReleaseKey -> Handle -> ResourceT (ErrorT FTPError IO) ()
cleanUp release_control release_data handle= do
  release release_data
  readExpected handle 226
  writeLine handle "QUIT"
  readExpected handle 221
  release release_control
  return ()

setupHandleForFTP :: URI -> IOMode -> ResourceT (ErrorT FTPError IO) (Handle, Handle, String, ReleaseKey, ReleaseKey)
setupHandleForFTP URI { uriScheme = scheme
                      , uriAuthority = authority
                      , uriPath = path
                      } iomode = do 
  s <- liftIO $ connectTCP host (PortNum (hton_16 port))
  h <- liftIO $ socketToHandle s ReadWriteMode
  liftIO $ hSetBuffering h LineBuffering
  release_control <- register $ liftIO $ hClose h
  readExpected h 220
  writeLine h $ "USER " ++ user
  readExpected h 331
  writeLine h $ "PASS " ++ pass
  readExpected h 230
  writeLine h "TYPE I"
  readExpected h 200
  writeLine h "PASV"
  pasv_response <- readExpected h 227
  let (pasvhost, pasvport) = parsePasvString pasv_response
  ds <- liftIO $ connectTCP pasvhost (PortNum (hton_16 pasvport))
  dh <- liftIO $ socketToHandle ds iomode
  liftIO $ hSetBuffering h $ BlockBuffering Nothing
  release_data <- register $ liftIO $ hClose dh
  return (h, dh, path, release_control, release_data)
  where (host, port, user, pass) = case authority of
          Nothing -> undefined
          Just (URIAuth userInfo regName port) ->
            ( regName
            , if null port then 21 else read (tail port)
            , if null userInfo then "anonymous" else takeWhile (\ l -> l /= ':' && l /= '@') userInfo
            , if null userInfo || not (':' `elem` userInfo) then "" else init $ tail $ (dropWhile (/= ':')) userInfo
            )
        parsePasvString s = (pasvhost, pasvport)
          where pasvhost = (show ip1) ++ "." ++ (show ip2) ++ "." ++ (show ip3) ++ "." ++ (show ip4)
                pasvport = (fromIntegral port1) `shiftL` 8 + (fromIntegral port2)
                (ip1, ip2, ip3, ip4, port1, port2) = read $ (++ ")") $ (takeWhile (/= ')')) $ (dropWhile (/= '(')) s :: (Int, Int, Int, Int, Int, Int)

-- | Returns (Handle of the control connection, the Sink itself
-- a destructor for the control connection, and a destructor for the data connection).
--
-- The caller should use the handle to check for return codes, and release the ReleaseKeys when appropriate.
putFTPFile :: URI -> ResourceT (ErrorT FTPError IO) (Handle, (Sink ByteString (ErrorT FTPError IO) ()), ReleaseKey, ReleaseKey)
putFTPFile uri = do
  (h, dh, path, release_control, release_data) <- setupHandleForFTP uri WriteMode
  writeLine h $ "STOR " ++ path
  readExpected h 150
  return $ (h, sinkHandle dh, release_control, release_data)

-- | Returns (Handle of the control connection, the Source itself,
-- a destructor for the control connection, and a destructor for the data connection).
--
-- The caller should use the handle to check for return codes, and release the ReleaseKeys when appropriate.
getFTPFile :: URI -> ResourceT (ErrorT FTPError IO) (Handle, (Source (ErrorT FTPError IO) ByteString), ReleaseKey, ReleaseKey)
getFTPFile uri = do
  (h, dh, path, release_control, release_data) <- setupHandleForFTP uri ReadMode
  writeLine h $ "RETR " ++ path
  readExpected h 150
  return $ (h, sourceHandle dh, release_control, release_data)
