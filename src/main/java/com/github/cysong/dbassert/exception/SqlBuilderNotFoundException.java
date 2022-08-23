package com.github.cysong.dbassert.exception;

/**
 * not matched SqlBuilder found error
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class SqlBuilderNotFoundException extends RuntimeException {

    public SqlBuilderNotFoundException(String message) {
        super(message);
    }
}
