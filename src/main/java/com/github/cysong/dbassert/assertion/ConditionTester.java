package com.github.cysong.dbassert.assertion;

import com.github.cysong.dbassert.constant.Comparator;
import com.github.cysong.dbassert.expression.Boundary;
import com.github.cysong.dbassert.utitls.Utils;

import java.util.*;
import java.util.function.Predicate;

/**
 * test condition defined by {@link com.github.cysong.dbassert.expression.Condition}
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
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
                return isTrue(actual);
            case IS_FALSE:
                return isFalse(actual);
            case LESS_THAN:
                return compare(actual, expected) < 0;
            case LESS_THAN_OR_EQUAL:
                return compare(actual, expected) <= 0;
            case GREATER_THAN:
                return compare(actual, expected) > 0;
            case GREATER_THAN_OR_EQUAL:
                return compare(actual, expected) >= 0;
            case BETWEEN:
                return between(actual, (Boundary<?>) expected);
            case IN:
                return in(actual, expected);
            case NOT_IN:
                return notIn(actual, expected);
            case MATCHES:
                return matches(actual, expected);
            case NOT_MATCH:
                return notMatch(actual, expected);
            case CONTAINS:
                return contains(actual, expected);
            case NOT_CONTAIN:
                return notContain(actual, expected);
            case LIST_IS_EMPTY:
                return Utils.isEmpty((List) actual);
            case LIST_NOT_EMPTY:
                return Utils.isNotEmpty((List) actual);
            case LIST_HAS_SIZE:
                return actual != null && compare(((List) actual).size(), expected) == 0;
            case LIST_EQUALS:
                return listEquals(actual, expected);
            case LIST_EQUALS_AT_ANY_ORDER:
                return listEqualsAtAnyOrder(actual, expected);
            case LIST_NOT_EQUAL:
                return listNotEqual(actual, expected);
            case LIST_CONTAINS:
                return listContains(actual, expected);
            case LIST_NOT_CONTAIN:
                return listNotContain(actual, expected);
            case LIST_CONTAINS_ANY:
                return listContainsAny(actual, expected);
            case LIST_IS_ORDERED_ASC:
                return listIsOrderedAsc(actual);
            case LIST_IS_ORDERED_DESC:
                return listIsOrderedDesc(actual);
            case LIST_MATCHES:
                return listMatches(actual, expected);
            case LIST_NOT_MATCH:
                return listNotMatch(actual, expected);
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
        return compare0(actual, expected);
    }

    private static int compare0(Object actual, Object expected) {
        assert actual != null;
        assert expected != null;
        Class<?> clazz = actual.getClass();
        try {
            switch (clazz.getName()) {
                case "java.lang.String":
                    return ((String) actual).compareTo(Converter.toString(expected));
                case "java.lang.Integer":
                    return Integer.compare((Integer) actual, Converter.toInteger(expected));
                case "java.lang.Long":
                    return Long.compare((Long) actual, Converter.toLong(expected));
                case "java.lang.Short":
                    return Short.compare((Short) actual, Converter.toShort(expected));
                case "java.lang.Float":
                    return Float.compare((Float) actual, Converter.toFloat(expected));
                case "java.lang.Double":
                    return Double.compare((Double) actual, Converter.toDouble(expected));
                case "java.lang.Boolean":
                    return Boolean.compare((Boolean) actual, Converter.toBoolean(expected));
                default:
                    throw new IllegalArgumentException("Unsupported data type:" + clazz.getName());
            }
        } catch (NumberFormatException e) {
            throw new ClassCastException(String.format("Expected value [%s] cannot be cast to %s", expected, clazz));
        }
    }

    public static boolean isTrue(Object actual) {
        if (actual == null) {
            return false;
        }
        if (actual instanceof Boolean) {
            return (Boolean) actual;
        }
        if (actual instanceof Integer) {
            return (Integer) actual == 1;
        }
        if (actual instanceof Long) {
            return (Long) actual == 1;
        }
        String s = String.valueOf(actual);
        return s.equalsIgnoreCase("true") || s.equals("1");
    }

    public static boolean isFalse(Object actual) {
        if (actual == null) {
            return false;
        }
        if (actual instanceof Boolean) {
            return !(Boolean) actual;
        }
        if (actual instanceof Integer) {
            return (Integer) actual == 0;
        } else if (actual instanceof Long) {
            return (Long) actual == 0;
        }
        String s = String.valueOf(actual);
        return s.equalsIgnoreCase("false") || s.equals("0");
    }

    public static boolean in(Object actual, Object expected) {
        Collection<?> cc = actual instanceof Collection ? (Collection<?>) actual : Arrays.asList(actual);
        for (Object item : cc) {
            boolean in = false;
            Iterator<?> eit = ((Iterable<?>) expected).iterator();
            while (eit.hasNext()) {
                if (Objects.equals(item, eit.next())) {
                    in = true;
                    break;
                }
            }
            if (!in) {
                return false;
            }
        }
        return true;
    }

    public static boolean notIn(Object actual, Object expected) {
        Collection<?> cc = actual instanceof Collection ? (Collection<?>) actual : Arrays.asList(actual);
        for (Object item : cc) {
            Iterator<?> eit = ((Iterable<?>) expected).iterator();
            while (eit.hasNext()) {
                if (Objects.equals(item, eit.next())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean between(Object actual, Boundary<?> boundary) {
        if (actual == null) {
            return false;
        }
        boolean result = true;
        if (boundary.isExcludeMin()) {
            result = result && (compare0(actual, boundary.getMin()) > 0);
        } else {
            result = result && (compare0(actual, boundary.getMin()) >= 0);
        }
        if (boundary.isExcludeMax()) {
            result = result && (compare0(actual, boundary.getMax()) < 0);
        } else {
            result = result && (compare0(actual, boundary.getMax()) <= 0);
        }
        return result;
    }

    public static boolean contains(Object actual, Object expected) {
        if (actual == null) {
            return false;
        }
        if (actual instanceof String) {
            return ((String) actual).contains((String) expected);
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
        } else {
            throw new IllegalArgumentException(Comparator.CONTAINS.name() + " not support type:" + actual.getClass().getName());
        }
    }

    public static boolean matches(Object actual, Object expected) {
        Predicate<Object> predicate = (Predicate<Object>) expected;
        if (actual instanceof Collection) {
            Collection collection = (Collection) actual;
            return collection.stream().allMatch(predicate);
        } else {
            return predicate.test(actual);
        }
    }

    public static boolean notMatch(Object actual, Object expected) {
        Predicate<Object> predicate = (Predicate<Object>) expected;
        if (actual instanceof Collection) {
            Collection collection = (Collection) actual;
            return !collection.stream().anyMatch(predicate);
        } else {
            return !predicate.test(actual);
        }
    }

    public static boolean listEquals(Object actual, Object expected) {
        if (actual == null) {
            return expected == null || !((Iterable<?>) expected).iterator().hasNext();
        }
        Iterator<?> a = ((Iterable<?>) actual).iterator();
        Iterator<?> e = ((Iterable<?>) expected).iterator();
        while (a.hasNext()) {
            if (!(e.hasNext() && Objects.equals(a.next(), e.next()))) {
                return false;
            }
        }
        return !e.hasNext();
    }

    public static boolean listNotEqual(Object actual, Object expected) {
        if (actual == null) {
            return !(expected != null && ((Iterable<?>) expected).iterator().hasNext());
        }
        Iterator<?> a = ((Iterable<?>) actual).iterator();
        Iterator<?> e = ((Iterable<?>) expected).iterator();
        while (a.hasNext()) {
            if (!(e.hasNext() && Objects.equals(a.next(), e.next()))) {
                return true;
            }
        }
        return e.hasNext();
    }

    public static boolean listEqualsAtAnyOrder(Object actual, Object expected) {
        if (actual == null) {
            return expected == null || !((Iterable<?>) expected).iterator().hasNext();
        }
        Iterator<?> a = ((Iterable<?>) actual).iterator();
        List<Object> eList = new ArrayList<>();
        ((Iterable<?>) expected).forEach(item -> eList.add(item));
        while (a.hasNext()) {
            Object aValue = a.next();
            Iterator<Object> e = eList.iterator();
            boolean match = false;
            while (e.hasNext()) {
                if (Objects.equals(aValue, e.next())) {
                    e.remove();
                    match = true;
                    break;
                }
            }
            if (!match) {
                return false;
            }
        }
        return eList.size() == 0;
    }

    public static boolean listContains(Object actual, Object expected) {
        if (actual == null) {
            return false;
        }
        List<?> aList = (List) actual;
        if (!(expected instanceof Iterable)) {
            return aList.contains(expected);
        }
        Iterator<?> e = ((Iterable<?>) expected).iterator();
        while (e.hasNext()) {
            if (!aList.contains(e.next())) {
                return false;
            }
        }
        return true;
    }

    public static boolean listNotContain(Object actual, Object expected) {
        if (actual == null) {
            return true;
        }
        List<?> aList = (List) actual;
        if (!(expected instanceof Iterable)) {
            return !aList.contains(expected);
        }
        Iterator<?> e = ((Iterable<?>) expected).iterator();
        while (e.hasNext()) {
            if (aList.contains(e.next())) {
                return false;
            }
        }
        return true;
    }

    public static boolean listContainsAny(Object actual, Object expected) {
        if (actual == null) {
            return false;
        }
        List<?> aList = (List) actual;
        if (!(expected instanceof Iterable)) {
            return aList.contains(expected);
        }
        Iterator<?> e = ((Iterable<?>) expected).iterator();
        while (e.hasNext()) {
            if (aList.contains(e.next())) {
                return true;
            }
        }
        return false;
    }

    public static boolean listIsOrderedAsc(Object actual) {
        if (actual == null) {
            return true;
        }
        List<?> aList = (List<?>) actual;
        for (int i = 0; i < aList.size() - 1; i++) {
            if (compare(aList.get(i), aList.get(i + 1)) == 1) {
                return false;
            }
        }
        return true;
    }

    public static boolean listIsOrderedDesc(Object actual) {
        if (actual == null) {
            return true;
        }
        List<?> aList = (List<?>) actual;
        for (int i = 0; i < aList.size() - 1; i++) {
            if (compare(aList.get(i), aList.get(i + 1)) == -1) {
                return false;
            }
        }
        return true;
    }

    public static boolean listMatches(Object actual, Object expected) {
        List<?> param = actual == null ? new ArrayList<>(0) : (List<?>) actual;
        Predicate<List> predicate = (Predicate<List>) expected;
        return predicate.test(param);
    }

    public static boolean listNotMatch(Object actual, Object expected) {
        List<?> param = actual == null ? new ArrayList<>(0) : (List<?>) actual;
        Predicate<List> predicate = (Predicate<List>) expected;
        return !predicate.test(param);
    }

}
