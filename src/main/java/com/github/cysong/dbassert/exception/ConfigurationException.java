package com.github.cysong.dbassert.exception;

/**
 * configuration exception
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class ConfigurationException extends RuntimeException {

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
