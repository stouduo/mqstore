package com.stouduo.mq.util;

public class StringUtil {
    public static boolean isEmpty(String str) {
        return str == null || "".equalsIgnoreCase(str);
    }
}
