package com.vmturbo.auth.api.authorization.jwt;

import java.security.PublicKey;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.kvstore.IApiAuthStore;

/**
 * Intercepting incoming calls before that are dispatched by {@link ServerCallHandler}.
 * Use this mechanism to add cross-cutting behavior to server-side calls to:
 * <ul>
 * <li>Retrieve JWT token from caller</li>
 * <li>Validate JWT token with public key</li>
 * <li>Store caller subject to context {@link Context}</li>
 * </ul>
 */
public class JwtServerInterceptor implements ServerInterceptor {
    public static final int ALLOWED_CLOCK_SKEW_SECONDS = 60;
    private static final ServerCall.Listener NOOP_LISTENER = new ServerCall.Listener() {
    };
    private static final Logger logger = LogManager.getLogger();
    private final PublicKey key;

    /**
     * Constructs the interceptor with public key.
     *
     * @param apiAuthStore the API Auth store
     */
    public JwtServerInterceptor(@Nonnull IApiAuthStore apiAuthStore) {
        Objects.requireNonNull(apiAuthStore);
        key = JWTKeyCodec.decodePublicKey(apiAuthStore.retrievePublicKey());
    }

    /**
     * Intercept and validate JWT token.
     * If JWT token is valid, extract the caller subject and store it to context {@link Context}.
     * Otherwise, close the server call with error code UNAUTHENTICATED.
     *
     *
     * @param call object to receive response messages
     * @param next next processor in the interceptor chain
     * @return listener for processing incoming messages for {@code call}, never {@code null}.
     */
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(@Nonnull ServerCall<ReqT, RespT> call,
                                                                 @Nonnull Metadata metadata,
                                                                 @Nonnull ServerCallHandler<ReqT, RespT> next) {
        final String jwt = metadata.get(SecurityConstant.JWT_METADATA_KEY);
        if (jwt == null) {
            // It should be uncommented when JWT token is always required.
            // serverCall.close(Status.UNAUTHENTICATED.withDescription("JWT Token is missing from Metadata"), metadata);
            logger.debug("gRPC request doesn't have JWT token.");
            return NOOP_LISTENER;
        }
        Context ctx;
        try {
            logger.debug("Received JWT token: " + jwt);
            JWTAuthorizationToken token = new JWTAuthorizationToken(jwt);
            Claims claims = Jwts.parser()
                    .setAllowedClockSkewSeconds(ALLOWED_CLOCK_SKEW_SECONDS)
                    .setSigningKey(key)
                    .parseClaimsJws(jwt)
                    .getBody();
            ctx = Context.current().withValue(SecurityConstant.USER_ID_CTX_KEY, claims.getSubject());
        } catch (RuntimeException e) {
            logger.error("Verification failed - Unauthenticated! With error message: " + e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage()).withCause(e), metadata);
            return NOOP_LISTENER;
        }
        return Contexts.interceptCall(ctx, call, metadata, next);
    }
}
