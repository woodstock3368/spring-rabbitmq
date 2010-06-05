package com.rabbitmq.spring;

public class UnRoutableException extends RuntimeException {

    public UnRoutableException() {
    }

    public UnRoutableException(String message) {
        super(message);
    }

    public UnRoutableException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnRoutableException(Throwable cause) {
        super(cause);
    }
}
