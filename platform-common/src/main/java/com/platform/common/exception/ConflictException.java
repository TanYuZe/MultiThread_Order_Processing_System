package com.platform.common.exception;

public class ConflictException extends PlatformException {

    public ConflictException(String message) {
        super("CONFLICT", message);
    }
}
