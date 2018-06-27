package io.openmessaging.util;

public class StringUtil {
    public static boolean isEmpty(String str) {
        return str == null || "".equalsIgnoreCase(str);
    }
}
