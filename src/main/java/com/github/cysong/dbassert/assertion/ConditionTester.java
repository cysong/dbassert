package com.github.cysong.dbassert.assertion;

import com.github.cysong.dbassert.constant.Comparator;
import com.github.cysong.dbassert.expression.Boundary;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;

public class ConditionTester {

    public static boolean test(Comparator comparator, Object actual, Object expected) {
        switch (comparator) {
            case EQUAL:
                return compare(actual, expected) == 0;
            case NOT_EQUAL:
                return compare(actual, expected) != 0;
            case NULL:
                return actual == null;
            case NOT_NULL:
                return actual != null;
            case IS_TRUE:
                return actual == null ? false : isTrue(actual);
            case IS_FALSE:
                return actual == null ? false : !isTrue(actual);
            case LESS_THAN:
                return compare(actual, expected) < 0;
            case LESS_THAN_OR_EQUAL:
                return compare(actual, expected) <= 0;
            case GREATER_THAN:
                return compare(actual, expected) > 0;
            case GREATER_THAN_OR_EQUAL:
                return compare(actual, expected) >= 0;
            case BETWEEN:
                return between(actual, (Boundary) expected);
            case IN:
                return in(actual, expected);
            case NOT_IN:
                return notIn(actual, expected);
            case MATCHES:
                return matches(actual, expected);
            case NOT_MATCH:
                return notMatch(actual, expected);
            case ANY_MATCH:
                return anyMatch(actual, expected);
            case CONTAINS:
                return contains(actual, expected);
            case NOT_CONTAIN:
                return notContain(actual, expected);
            default:
                throw new IllegalArgumentException("Unrecognized Comparator:" + comparator.name());
        }
    }

    public static int compare(Object actual, Object expected) {
        if (actual == null) {
            if (expected == null) {
                return 0;
            } else {
                return -1;
            }
        }
        return compareNotNull(actual, expected);
    }

    public static int compareNotNull(Object actual, Object expected) {
        assert actual != null;
        assert expected != null;
        Class clazz = actual.getClass();
        switch (clazz.getName()) {
            case "java.lang.String":
                return ((String) actual).compareTo((String) expected);
            case "java.lang.Integer":
                Integer properExp = null;
                if (expected != null) {
                    if (expected instanceof Integer) {
                        properExp = (Integer) expected;
                    } else if (expected instanceof Long) {
                        properExp = ((Long) expected).intValue();
                    } else {
                        properExp = Integer.valueOf(expected.toString());
                    }
                }
                return Integer.compare((Integer) actual, properExp);
            case "java.lang.Long":
                return Long.compare((Long) actual, (Long) expected);
            case "java.lang.Short":
                return Short.compare((Short) actual, (Short) expected);
            case "java.lang.Float":
                return Float.compare((Float) actual, (Float) expected);
            case "java.lang.Double":
                return Double.compare((Double) actual, (Double) expected);
            case "java.lang.Boolean":
                return Boolean.compare((Boolean) actual, (Boolean) expected);
            default:
                throw new IllegalArgumentException("Unsupported data type:" + clazz.getName());
        }
    }

    public static boolean isTrue(Object actual) {
        if (actual == null) {
            return false;
        }
        if (actual instanceof Integer) {
            return (Integer) actual != 0;
        } else if (actual instanceof Long) {
            return (Long) actual != 0;
        } else if (actual instanceof String) {
            return Boolean.parseBoolean((String) actual);
        } else {
            return Boolean.parseBoolean(actual.toString());
        }
    }

    public static boolean in(Object actual, Object expected) {
        Collection cc = actual instanceof Collection ? (Collection) actual : Arrays.asList(actual);
        for (Object item : cc) {
            boolean in = false;
            Iterator eit = ((Iterable) expected).iterator();
            while (eit.hasNext()) {
                if (Objects.equals(item, eit.next())) {
                    in = true;
                    continue;
                }
            }
            if (!in) {
                return false;
            }
        }
        return true;
    }

    public static boolean notIn(Object actual, Object expected) {
        Collection cc = actual instanceof Collection ? (Collection) actual : Arrays.asList(actual);
        for (Object item : cc) {
            Iterator eit = ((Iterable) expected).iterator();
            while (eit.hasNext()) {
                if (Objects.equals(item, eit.next())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean between(Object actual, Boundary boundary) {
        if (actual == null) {
            return false;
        }
        boolean result = true;
        if (boundary.isExcludeMin()) {
            result = result && compareNotNull(actual, boundary.getMin()) > 0;
        } else {
            result = result && compareNotNull(actual, boundary.getMin()) >= 0;
        }
        if (boundary.isExcludeMax()) {
            result = result && compareNotNull(actual, boundary.getMax()) < 0;
        } else {
            result = result && compareNotNull(actual, boundary.getMax()) <= 0;
        }
        return result;
    }

    public static boolean contains(Object actual, Object expected) {
        if (actual == null) {
            return false;
        }
        if (actual instanceof String) {
            return ((String) actual).contains((String) expected);
        } else if (actual instanceof Collection) {
            Collection coll = (Collection) actual;
            Iterator it = ((Iterable) expected).iterator();
            while (it.hasNext()) {
                if (!coll.contains(it.next())) {
                    return false;
                }
            }
            return true;
        } else {
            throw new IllegalArgumentException(Comparator.CONTAINS.name() + " not support type:" + actual.getClass().getName());
        }
    }

    public static boolean notContain(Object actual, Object expected) {
        if (actual == null) {
            return false;
        }
        if (actual instanceof String) {
            return !((String) actual).contains((String) expected);
        } else if (actual instanceof Collection) {
            Collection coll = (Collection) actual;
            Iterator it = ((Iterable) expected).iterator();
            while (it.hasNext()) {
                if (coll.contains(it.next())) {
                    return false;
                }
            }
            return true;
        } else {
            throw new IllegalArgumentException(Comparator.CONTAINS.name() + " not support type:" + actual.getClass().getName());
        }
    }

    public static boolean matches(Object actual, Object expected) {
        Predicate predicate = (Predicate) expected;
        if (actual instanceof Collection) {
            Collection collection = (Collection) actual;
            return collection.stream().allMatch(predicate);
        } else {
            return predicate.test(actual);
        }
    }

    public static boolean notMatch(Object actual, Object expected) {
        Predicate predicate = (Predicate) expected;
        if (actual instanceof Collection) {
            Collection collection = (Collection) actual;
            return !collection.stream().anyMatch(predicate);
        } else {
            return !predicate.test(actual);
        }
    }

    public static boolean anyMatch(Object actual, Object expected) {
        Predicate predicate = (Predicate) expected;
        if (actual instanceof Collection) {
            Collection collection = (Collection) actual;
            return collection.stream().anyMatch(predicate);
        } else {
            return predicate.test(actual);
        }
    }
}
