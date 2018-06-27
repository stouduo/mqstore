package io.openmessaging.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * String压缩/解压
 */
public class StringCompress {

    /**
     * 压缩字符串
     *
     * @param str 压缩的字符串
     * @return 压缩后的字符串
     */
    public static byte[] compress(String str) {

        if (StringUtil.isEmpty(str)) {
            return new byte[0];
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPOutputStream os = new GZIPOutputStream(bos)) {
            os.write(str.getBytes("UTF-8")); // 写入输出流
            return bos.toByteArray();
        } catch (Exception ex) {
            return new byte[0];
        }
    }

    /**
     * 解压缩字符串
     *
     * @param body 解压缩的字符串
     * @return 解压后的字符串
     */
    public static String decompress(byte[] body) {

        byte[] buf;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(body);
             ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPInputStream is = new GZIPInputStream(bis)) {
            buf = new byte[1024];
            int len = 0;
            while ((len = is.read(buf)) != -1) { // 将未压缩数据读入字节数组  
                // 将指定 byte 数组中从偏移量 off 开始的 len 个字节写入此byte数组输出流  
                bos.write(buf, 0, len);
            }
            return new String(bos.toByteArray(), "UTF-8"); // 通过解码字节将缓冲区内容转换为字符串
        } catch (Exception ex) {
            buf = null;
            return "";
        }
    }

}