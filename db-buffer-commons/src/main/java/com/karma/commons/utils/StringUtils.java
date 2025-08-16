package com.karma.commons.utils;

public class StringUtils {

    public static final char FILENAME_SEPRATOR = '_';

    public static boolean isEmpty(String value) { return value == null || value.isEmpty(); }

    public static boolean isNumber(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c < 0 || c > 9) && c != '.') {
                return false;
            }
        }
        return true;
    }

    public static String camelCaseToFileName(String source) {
        StringBuilder builder = new StringBuilder(source.length() + 3);    // 额外增加字符空间（至少要保证额外 +1）
        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            if (isUpperCase(c)) {
                builder.append(FILENAME_SEPRATOR);
                builder.append(toLowerCase(c));
            } else {
                builder.append(c);
            }
        }

        if (builder.charAt(0) == FILENAME_SEPRATOR) {
            return builder.substring(1);
        } else {
            return builder.toString();
        }
    }

    private static boolean isUpperCase(char c) {
        return c >= 'A' && c <= 'Z';
    }

    private static char toLowerCase(char c) {
        return (char) (c + 32);
    }
}
