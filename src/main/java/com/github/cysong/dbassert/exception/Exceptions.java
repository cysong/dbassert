package com.github.cysong.dbassert.exception;

/**
 * exception utils
 *
 * @author cysong
 * @date 2022/8/26 14:38
 **/
public class Exceptions {

    public static void check(Throwable throwable) {
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        throw new RuntimeException(throwable);
    }
}
