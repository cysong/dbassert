package com.github.cysong.dbassert.assertion;


import java.math.BigDecimal;
import java.math.BigInteger;

public class Converter {

    public static Integer toInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Long) {
            Long l = (Long) value;
            if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                return l.intValue();
            }
            throw new NumberFormatException();
        }
        if (value instanceof Short || value instanceof Float || value instanceof Double) {
            if (((Number) value).intValue() == ((Number) value).doubleValue()) {
                return ((Number) value).intValue();
            }
        }
        return Integer.valueOf(String.valueOf(value));
    }

    public static Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        if (value instanceof Short || value instanceof Float || value instanceof Double) {
            if (((Number) value).longValue() == ((Number) value).doubleValue()) {
                return ((Number) value).longValue();
            }
        }
        return Long.valueOf(String.valueOf(value));
    }

    public static Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            if (((Long) value).longValue() == ((Long) value).doubleValue()) {
                return ((Long) value).doubleValue();
            }
            throw new NumberFormatException();
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
            throw new NumberFormatException();
        }
        String s = String.valueOf(value);
        if (s.equalsIgnoreCase("true") || s.equals("1")) {
            return true;
        } else if (s.equalsIgnoreCase("false") || s.equals("0")) {
            return false;
        }
        throw new NumberFormatException();
    }

    public static BigInteger toBigInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }
        if (value instanceof Integer) {
            return BigInteger.valueOf((Integer) value);
        }
        if (value instanceof Long) {
            return BigInteger.valueOf((Long) value);
        }
        return new BigInteger(String.valueOf(value));
    }

    public static BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Integer) {
            return BigDecimal.valueOf((Integer) value);
        }
        if (value instanceof Long) {
            return BigDecimal.valueOf((Long) value);
        }
        if (value instanceof Short) {
            return BigDecimal.valueOf((Short) value);
        }
        if (value instanceof Float) {
            return BigDecimal.valueOf((Float) value);
        }
        if (value instanceof Double) {
            return BigDecimal.valueOf((Double) value);
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        return new BigDecimal(String.valueOf(value));
    }

    public static String toString(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

}
