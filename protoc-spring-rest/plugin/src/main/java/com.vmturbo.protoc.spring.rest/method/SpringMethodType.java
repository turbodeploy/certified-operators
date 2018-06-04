package com.vmturbo.protoc.spring.rest.method;

import javax.annotation.Nonnull;

/**
 * Represents the various HTTP method types, and the associated
 * Spring enum.
 *
 * This only represents the method types supported by the protobuf compiler framework.
 */
enum SpringMethodType {
    POST("RequestMethod.POST"),
    PUT("RequestMethod.PUT"),
    PATCH("RequestMethod.PATCH"),
    GET("RequestMethod.GET"),
    DELETE("RequestMethod.DELETE");

    private final String springMethodType;

    SpringMethodType(final String springMethodType) {
        this.springMethodType = springMethodType;
    }

    @Nonnull
    public String getType() {
        return springMethodType;
    }
}
