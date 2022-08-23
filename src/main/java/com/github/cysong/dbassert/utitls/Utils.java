package com.github.cysong.dbassert.utitls;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * common utils
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class Utils {

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isNotEmpty(Collection collection) {
        return !isEmpty(collection);
    }

    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    public static boolean isNotEmpty(Map map) {
        return !isEmpty(map);
    }

    public static boolean isEmpty(CharSequence cs) {
        return cs != null && cs.length() == 0;
    }

    public static boolean isNotEmpty(CharSequence cs) {
        return isEmpty(cs);
    }

    public static boolean isBlank(CharSequence cs) {
        int strLen;
        if (cs != null && (strLen = cs.length()) != 0) {
            for (int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(cs.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    public static boolean isNotBlank(CharSequence cs) {
        return !isBlank(cs);
    }

    public static void sleep(long milliseconds) {
        try {
            TimeUnit.MILLISECONDS.sleep(milliseconds);
        } catch (InterruptedException e) {
        }
    }

    public static String readInputStreamAsString(InputStream inputStream) throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append(System.lineSeparator());
            }
        }
        return resultStringBuilder.toString();
    }

    public static String trim(String source, char c) {
        if (source == null || source.length() == 0) {
            return source;
        }
        char[] val = source.toCharArray();
        int len = val.length;
        int i = 0;
        while ((i < len) && (val[i] == c)) {
            i++;
        }
        while ((i < len) && val[len - 1] == c) {
            len--;
        }
        if (i > 0 || len < val.length) {
            return new String(val, i, len - i);
        }
        return source;
    }
}
