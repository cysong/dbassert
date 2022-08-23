package com.github.cysong.dbassert.exception;

/**
 * data format error
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class DataFormatException extends RuntimeException {

    public DataFormatException(String message) {
        super(message);
    }
}
