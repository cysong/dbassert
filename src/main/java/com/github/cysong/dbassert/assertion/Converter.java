package com.github.cysong.dbassert.assertion;


import com.github.cysong.dbassert.exception.DataFormatException;

public class Converter {

    public static Integer toInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.valueOf(String.valueOf(value));
    }

    public static Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.valueOf(String.valueOf(value));
    }

    public static Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.valueOf(String.valueOf(value));
    }

    public static Float toFloat(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return Float.valueOf(String.valueOf(value));
    }

    public static Short toShort(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        return Short.valueOf(String.valueOf(value));
    }

    public static Boolean toBoolean(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer || value instanceof Long) {
            Integer i = ((Number) value).intValue();
            if (i == 1 || i == 0) {
                return i == 1;
            }
            throw new DataFormatException("Data can not be convert to boolean:" + value);
        }
        String s = String.valueOf(value);
        if (s.equalsIgnoreCase("true") || s.equals("1")) {
            return true;
        } else if (s.equalsIgnoreCase("false") || s.equals("0")) {
            return false;
        }
        throw new DataFormatException("Data can not be convert to boolean:" + value);
    }

    public static String toString(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

}
