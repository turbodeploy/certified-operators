package com.vmturbo.auth.api.authorization.jwt;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Context;
import io.grpc.Metadata;

/**
 * Define security related constants.
 */
public class SecurityConstant {
    /**
     * key for user id
     */
    public static final Context.Key<String> USER_ID_CTX_KEY = Context.key("userId");

    /**
     * key for user's IP address
     */

    public static final Context.Key<String> USER_IP_ADDRESS_KEY = Context.key("userIpAddress");
    ;
    /**
     * key for JWT token
     */
    public static final Metadata.Key<String> JWT_METADATA_KEY = Metadata.Key.of("jwt", ASCII_STRING_MARSHALLER);
}
