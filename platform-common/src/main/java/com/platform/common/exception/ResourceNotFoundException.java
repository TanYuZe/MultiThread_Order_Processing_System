package com.platform.common.exception;

public class ResourceNotFoundException extends PlatformException {

    public ResourceNotFoundException(String resourceType, Object id) {
        super("RESOURCE_NOT_FOUND", resourceType + " not found: " + id);
    }
}
