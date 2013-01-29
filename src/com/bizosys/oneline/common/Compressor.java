package com.bizosys.oneline.common;
import java.io.UnsupportedEncodingException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class Compressor
{
    public static byte[] compress(byte[] bytesToCompress)
    {        
        Deflater deflater = new Deflater();
        deflater.setInput(bytesToCompress);
        deflater.finish();

        byte[] bytesCompressed = new byte[Short.MAX_VALUE];
        int numberOfBytesAfterCompression = deflater.deflate(bytesCompressed);
        byte[] returnValues = new byte[numberOfBytesAfterCompression];

        System.arraycopy(bytesCompressed, 0, returnValues, 0, numberOfBytesAfterCompression);

        return returnValues;
    }

    public static byte[] compress(String stringToCompress)
    {        
        byte[] returnValues = null;

        try
        {
            returnValues = compress(stringToCompress.getBytes("UTF-8"));
        }
        catch (UnsupportedEncodingException ex)
        {
            ex.printStackTrace();
        }

        return returnValues;
    }

    public static byte[] decompress(byte[] bytesToDecompress)
    {
        int numberOfBytesToDecompress = bytesToDecompress.length;
        int compressionFactorMaxLikely = 3;
        int bufferSizeInBytes = numberOfBytesToDecompress * compressionFactorMaxLikely;

        Inflater inflater = new Inflater();
        inflater.setInput(bytesToDecompress, 0, numberOfBytesToDecompress);

        byte[] bytesDecompressed = new byte[bufferSizeInBytes];
        byte[] returnValues = null;

        try
        {
            int numberOfBytesAfterDecompression = inflater.inflate(bytesDecompressed);
            returnValues = new byte[numberOfBytesAfterDecompression];
            System.arraycopy(bytesDecompressed, 0, returnValues, 0, numberOfBytesAfterDecompression);            
        }
        catch (DataFormatException ex)
        {
            ex.printStackTrace();
        }

        inflater.end();
        return returnValues;
    }

    public static String decompressToString(byte[] bytesToDecompress)
    {    
        byte[] bytesDecompressed = decompress(bytesToDecompress);
        String returnValue = null;

        try
        {
            returnValue = new String(bytesDecompressed, 0, bytesDecompressed.length, "UTF-8");    
        }
        catch (UnsupportedEncodingException ex)
        {
            ex.printStackTrace();
        }

        return returnValue;
    }
}