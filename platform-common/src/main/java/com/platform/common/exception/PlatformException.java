package com.platform.common.exception;

/**
 * Base exception for all domain-level errors in this platform.
 * Extends RuntimeException — all domain exceptions are unchecked.
 */
public abstract class PlatformException extends RuntimeException {

    private final String errorCode;

    protected PlatformException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    protected PlatformException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
