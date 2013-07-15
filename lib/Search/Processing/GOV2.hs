{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Search.Processing.GOV2
  (
    GOV2Page(..)
  , govFileToTexts, govText
  ) where

import Codec.Compression.GZip (decompress)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import qualified Data.ByteString.Internal as BI
import Text.HTML.TagSoup
import Text.HTML.TagSoup.Match
import Data.ByteString.Char8 (pack)
import Foreign.Storable (peekElemOff, pokeElemOff)
import Foreign.Ptr (plusPtr, Ptr)
import Foreign.ForeignPtr (withForeignPtr)
import System.IO.Unsafe (unsafeDupablePerformIO)

import Data.Word (Word8, Word32)

import Data.Maybe

import qualified Data.Text as T
import Data.Text.Encoding
import Data.Text.Encoding.Error

data GOV2Page =
    GOV2Page
    { gov2PageText :: T.Text
    , gov2File :: FilePath
    , gov2PageOffset :: Word32
    , gov2PageLength :: Word32
    }

govText :: FilePath -> Int -> Int -> IO T.Text
govText path o l =
    do raw <- decompressed path
       let rawPage = BS.take l $ BS.drop o raw
       return $ rawPageToText rawPage

rawPageToText :: BS.ByteString -> T.Text
rawPageToText = decodeUtf8With lenientDecode . htmlToText . parseTags . extractHtmlBody

govFileToTexts :: FilePath -> IO [GOV2Page]
govFileToTexts path =
    do raw <- decompressed path
       let rawpages = stringsWithPrefix "<DOC>" raw
       let lengths = map (BS.length) rawpages
       let offsets = init $ scanl (+) 0 lengths
       let texts = map rawPageToText rawpages
       return $ map tupleToPage $ zip3 texts offsets lengths
    where tupleToPage (t,o,l) = GOV2Page
                                { gov2PageText = t
                                , gov2File = path
                                , gov2PageOffset = fromIntegral o
                                , gov2PageLength = fromIntegral l
                                }

htmlToText :: [Tag BS.ByteString] -> BS.ByteString
htmlToText = BS.concat . mapMaybe tagToText .
             stripTag "NOSCRIPT" . stripTag "noscript" .
             stripTag "SCRIPT" . stripTag "script" .
             stripTag "STYLE" . stripTag "style"

stripTag :: Eq a => a -> [Tag a] -> [Tag a]
stripTag = stripTagClosed

stripTagOpened :: Eq a => a -> [Tag a] -> [Tag a]
--stripTagOpened tag = stripTagClosed tag . tail . dropWhile (not . tagClose (==tag))
stripTagOpened _ [] = []
stripTagOpened tag (t:ts) =
    if tagClose (==tag) t
       then stripTagClosed tag ts
       else stripTagOpened tag ts

stripTagClosed :: Eq a => a -> [Tag a] -> [Tag a]
stripTagClosed _ [] = []
stripTagClosed tag (t:ts) =
    if tagOpen (==tag) (const True) t
       then stripTagOpened tag ts
       else t:stripTagClosed tag ts

tagToText :: Tag BS.ByteString -> Maybe BS.ByteString
tagToText (TagText str) = Just $ htmlStripWhitespace str
tagToText (TagOpen "br" _) = Just "\n"
tagToText (TagOpen "BR" _) = Just "\n"
tagToText (TagOpen "hr" _) = Just "\n"
tagToText (TagOpen "HR" _) = Just "\n"
tagToText (TagClose "title") = Just "\n\n"
tagToText (TagClose "TITLE") = Just "\n\n"
tagToText (TagClose "tr") = Just "\n"
tagToText (TagClose "TR") = Just "\n"
tagToText (TagClose "td") = Just " "
tagToText (TagClose "TD") = Just " "
tagToText (TagClose "table") = Just "\n"
tagToText (TagClose "TABLE") = Just "\n"
tagToText (TagClose "p") = Just "\n"
tagToText (TagClose "P") = Just "\n"
tagToText (TagClose "div") = Just "\n"
tagToText (TagClose "DIV") = Just "\n"
tagToText (TagClose "ul") = Just "\n\n"
tagToText (TagClose "UL") = Just "\n\n"
tagToText (TagOpen "li" _) = Just "\n* "
tagToText (TagOpen "LI" _) = Just "\n* "
tagToText (TagOpen "h1" _) = Just "\n"
tagToText (TagOpen "H1" _) = Just "\n"
tagToText (TagClose "h1") = Just "\n"
tagToText (TagClose "H1") = Just "\n"
tagToText (TagOpen "h2" _) = Just "\n"
tagToText (TagOpen "H2" _) = Just "\n"
tagToText (TagClose "h2") = Just "\n"
tagToText (TagClose "H2") = Just "\n"
tagToText (TagOpen "h3" _) = Just "\n"
tagToText (TagOpen "H3" _) = Just "\n"
tagToText (TagClose "h3") = Just "\n"
tagToText (TagClose "H3") = Just "\n"
tagToText (TagOpen "h4" _) = Just "\n"
tagToText (TagOpen "H4" _) = Just "\n"
tagToText (TagClose "h4") = Just "\n"
tagToText (TagClose "H4") = Just "\n"
tagToText (TagOpen "h5" _) = Just "\n"
tagToText (TagOpen "H5" _) = Just "\n"
tagToText (TagClose "h5") = Just "\n"
tagToText (TagClose "H5") = Just "\n"
tagToText (TagOpen "h6" _) = Just "\n"
tagToText (TagOpen "H6" _) = Just "\n"
tagToText (TagClose "h6") = Just "\n"
tagToText (TagClose "H6") = Just "\n"
tagToText _ = Nothing

htmlStripWhitespace :: BS.ByteString -> BS.ByteString
htmlStripWhitespace (BI.PS input s l) =
    unsafeDupablePerformIO $
    BI.createAndTrim l $ \op ->
    withForeignPtr input $ \i' ->
    do let i :: Ptr Word8
           !i = i' `plusPtr` s
       let go oi oo w
             | oi == l && w = do pokeElemOff op oo (0x20)
                                 return (oo+1)
             | oi == l = return oo
             | otherwise =
                 do !c <- fromIntegral `fmap` peekElemOff i oi
                    if isSpaceWord8 c
                       then go (oi+1) oo True
                       else if w
                               then do pokeElemOff op oo (0x20)
                                       pokeElemOff op (oo+1) c
                                       go (oi+1) (oo+2) False
                               else do pokeElemOff op oo c
                                       go (oi+1) (oo+1) False
       go 0 0 False

isSpaceWord8 :: Word8 -> Bool
isSpaceWord8 w =
    w == 0x20 ||
    w == 0x0A || -- LF, \n
    w == 0x09 || -- HT, \t
    w == 0x0C || -- FF, \f
    w == 0x0D || -- CR, \r
    w == 0x0B || -- VT, \v
    w == 0xA0

decompressed :: FilePath -> IO BS.ByteString
decompressed = (fmap (LBS.toStrict . decompress)) . LBS.readFile

extractHtmlBody :: BS.ByteString -> BS.ByteString
extractHtmlBody str =
       let (_, rest) = BS.breakSubstring closingHdrTag str
       in fst $ BS.breakSubstring closingDocTag $ BS.drop (BS.length closingHdrTag) rest
    where closingHdrTag = pack "</DOCHDR>\n"
          closingDocTag = pack "</DOC>\n"

stringsWithPrefix :: BS.ByteString -> BS.ByteString -> [BS.ByteString]
stringsWithPrefix pat str
    | BS.null pat         = error "empty pattern"
    | otherwise           = search str len (BS.unsafeDrop len str)
  where
    len = BS.length pat
    search !s !l !t
        | BS.null t             = [s]
        | pat `BS.isPrefixOf` t = (BS.unsafeTake l s) : search (BS.unsafeDrop l s) len (BS.unsafeDrop len t)
        | otherwise          = search s (l+1) (BS.unsafeTail t)
