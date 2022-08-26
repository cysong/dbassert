package com.github.cysong.dbassert.report;

/**
 * status of report step
 *
 * @author cysong
 * @date 2022/8/26 10:44
 **/
public enum Status {
    FAILED("failed"),
    BROKEN("broken"),
    PASSED("passed"),
    SKIPPED("skipped");

    public final String value;

    Status(final String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static Status fromValue(final String v) {
        for (Status c : Status.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }
}
