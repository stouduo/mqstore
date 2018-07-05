package io.openmessaging.util;

import java.io.ByteArrayOutputStream;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ByteUtil {
    public static byte[] int2Bytes(int num) {
        byte[] byteNum = new byte[4];
        for (int ix = 0; ix < 4; ++ix) {
            int offset = 32 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    public static void byteMerger(byte[] dest, byte[]... src) {
        int index = 0;
        for (byte[] bts : src) {
            System.arraycopy(bts, 0, dest, index, bts.length);
            index += bts.length;
        }
    }

    public static void int2Bytes(byte[] bytes, int offset, int num) {
        for (int ix = offset; ix < offset + 4; ++ix) {
            int o = 32 - (ix + 1) * 8;
            bytes[ix] = (byte) ((num >> o) & 0xff);
        }
    }

    public static int bytes2Int(byte[] byteNum, int offset) {
        int num = 0;
        for (int ix = offset; ix < offset + 4; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    public static byte int2OneByte(int num) {
        return (byte) (num & 0x000000ff);
    }

    public static int oneByte2Int(byte byteNum) {
        //针对正数的int
        return byteNum > 0 ? byteNum : (128 + (128 + byteNum));
    }

    public static byte[] long2Bytes(long num) {
        byte[] byteNum = new byte[8];
        for (int ix = 0; ix < 8; ++ix) {
            int offset = 64 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    public static void long2Bytes(byte[] ret, int offset, long num) {
        for (int ix = 0 + offset; ix < 8 + offset; ++ix) {
            int offs = 64 - (ix + 1) * 8;
            ret[ix] = (byte) ((num >> offs) & 0xff);
        }
    }

    public static long bytes2Long(byte[] byteNum, int offset) {
        long num = 0;
        for (int ix = offset; ix < offset + 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    public static byte[] compress(byte input[]) {
        Deflater compressor = new Deflater(1);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            compressor.setInput(input);
            compressor.finish();
            final byte[] buf = new byte[2048];
            while (!compressor.finished()) {
                int count = compressor.deflate(buf);
                bos.write(buf, 0, count);
            }
            return bos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            compressor.end();
        }
        return new byte[0];
    }

    public static byte[] uncompress(byte[] input) {
        Inflater decompressor = new Inflater();
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            decompressor.setInput(input);
            final byte[] buf = new byte[2048];
            while (!decompressor.finished()) {
                int count = decompressor.inflate(buf);
                bos.write(buf, 0, count);
            }
            return bos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            decompressor.end();
        }
        return new byte[0];
    }
}
