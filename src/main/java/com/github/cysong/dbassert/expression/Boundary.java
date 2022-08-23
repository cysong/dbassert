package com.github.cysong.dbassert.expression;


/**
 * boundary between min and max
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class Boundary<T> {
    private T min;
    private T max;
    private boolean excludeMin;
    private boolean excludeMax;

    public static Boundary create(Object min, Object max) {
        return new Boundary(min, false, max, false);
    }

    public static Boundary create(Object min, boolean excludeMin, Object max, boolean excludeMax) {
        return new Boundary(min, excludeMin, max, excludeMax);
    }

    private Boundary(T min, boolean excludeMin, T max, boolean excludeMax) {
        this.min = min;
        this.excludeMin = excludeMin;
        this.max = max;
        this.excludeMax = excludeMax;
    }

    @Override
    public String toString() {
        return (excludeMin ? "(" : "[") + min + "," + max + (excludeMax ? ")" : "]");
    }

    public T getMin() {
        return min;
    }

    public T getMax() {
        return max;
    }

    public boolean isExcludeMin() {
        return excludeMin;
    }

    public boolean isExcludeMax() {
        return excludeMax;
    }
}
