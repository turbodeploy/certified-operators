package com.vmturbo.auth.api.authorization.jwt;

import static com.vmturbo.auth.api.authorization.IAuthorizationVerifier.ROLE_CLAIM;
import static com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier.IP_ADDRESS_CLAIM;
import static com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier.UUID_CLAIM;

import java.security.PublicKey;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;

/**
 * Intercepting incoming calls before that are dispatched by {@link ServerCallHandler}.
 * Use this mechanism to add cross-cutting behavior to server-side calls to:
 * <ul>
 * <li>Retrieve JWT token from caller</li>
 * <li>Validate JWT token with public key</li>
 * <li>Store caller subject to context {@link Context}</li>
 * </ul>
 * This passes user identity and role in the JWT token so we can authorize and track
 * gRPC activity.
 * <p/>
 * Every component exposing a gRPC server should add a {@link JwtServerInterceptor} to
 * its list of {@link ServerInterceptor}s.
 */
public class JwtServerInterceptor implements ServerInterceptor {
    private static final int ALLOWED_CLOCK_SKEW_SECONDS = 60;
    private static final ServerCall.Listener NOOP_LISTENER = new ServerCall.Listener() {
    };
    private static final Logger logger = LogManager.getLogger();
    private final IAuthStore apiAuthStore;
    /**
     * Locks for retrieving public key from Auth store.
     */
    private final Object storeLock = new Object();
    private volatile PublicKey publicKey;

    /**
     * Constructs the interceptor with Auth Store.
     *
     * @param apiAuthStore the API Auth store
     */
    public JwtServerInterceptor(@Nonnull IAuthStore apiAuthStore) {
        Objects.requireNonNull(apiAuthStore);
        this.apiAuthStore = apiAuthStore;
    }

    /**
     * Intercept and validate JWT token.
     * If JWT token is valid, extract the caller subject and IP address, and store it to
     * context {@link Context}. Otherwise, close the server call with error code UNAUTHENTICATED.
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
        final Context ctx;
        if (jwt == null) {
            // It should be uncommented when JWT token is always required. For system calls, we will use system JWT token.
            // serverCall.close(Status.UNAUTHENTICATED.withDescription("JWT Token is missing from Metadata"), metadata);
            logger.debug("gRPC request doesn't have JWT token on call {}.", call.getMethodDescriptor().getFullMethodName());
            return next.startCall(call, metadata); // will use NOOP_LISTENER when JWT token is required
        }
        PublicKey key = getPublicKey();
        if (key == null) {
            // It should be uncommented when public key is always required.
            // serverCall.close(Status.UNAUTHENTICATED.withDescription("Public key is missing"), metadata);
            logger.error("Unable to retrieve public key.");
            return next.startCall(call, metadata); // will use NOOP_LISTENER when JWT token is required
        }

        try {
            logger.debug("Received JWT token: {} on call {}", jwt, call.getMethodDescriptor().getFullMethodName());
            JWTAuthorizationToken token = new JWTAuthorizationToken(jwt);
            Claims claims = Jwts.parser()
                    .setAllowedClockSkewSeconds(ALLOWED_CLOCK_SKEW_SECONDS)
                    .setSigningKey(key)
                    .parseClaimsJws(jwt)
                    .getBody();
            ctx = Context.current()
                    .withValue(SecurityConstant.USER_ID_CTX_KEY, claims.getSubject())
                    .withValue(SecurityConstant.USER_UUID_KEY, claims.get(UUID_CLAIM, String.class))
                    .withValue(SecurityConstant.USER_ROLES_KEY,
                            (List<String>) claims.getOrDefault(ROLE_CLAIM, Collections.EMPTY_LIST))
                    .withValue(SecurityConstant.USER_IP_ADDRESS_KEY, claims.get(IP_ADDRESS_CLAIM, String.class))
                    .withValue(SecurityConstant.USER_SCOPE_GROUPS_KEY,
                            (List<Long>) claims.getOrDefault(IAuthorizationVerifier.SCOPE_CLAIM,
                                    java.util.Collections.EMPTY_LIST))
                    .withValue(SecurityConstant.CONTEXT_JWT_KEY, jwt);
        } catch (RuntimeException e) {
            // TODO it should be sent to audit log.
            logger.error("Verification failed - Unauthenticated! With error message: " + e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage()).withCause(e), metadata);
            return NOOP_LISTENER;
        }

        // also install a handler that will convert turbo exception types to GRPC codes
        ServerCall.Listener<ReqT> delegate = Contexts.interceptCall(ctx, call, metadata, next);
        return new SimpleForwardingServerCallListener<ReqT>(delegate) {
            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (RuntimeException t) {
                    translateException(t);
                }
            }

            private void translateException(RuntimeException t) {
                final Status returnStatus;
                if (t instanceof UserAccessScopeException) {
                    returnStatus = Status.PERMISSION_DENIED;
                } else if (t instanceof UserAccessException) {
                    returnStatus = Status.PERMISSION_DENIED;
                } else {
                    // Exception is not related to Auth component logic. Rethrow it.
                    throw t;
                }
                logger.debug("gRPC call to " + call.getMethodDescriptor().getFullMethodName() +
                        " failed", t);
                call.close(returnStatus.withCause(t).withDescription(t.getMessage()), new Metadata());
            }
        };

    }

    /**
     * Get the public key for Auth store. We should NOT get the public key during component start up,
     * since the public key is not available in the Auth store if system is NOT initialize.
     *
     * @return public key.
     */
    private PublicKey getPublicKey() {
        if (publicKey == null) {
            synchronized (storeLock) {
                if (publicKey == null) {
                    String encodedPublicKey = apiAuthStore.retrievePublicKey();
                    if (encodedPublicKey != null) {
                        publicKey = JWTKeyCodec.decodePublicKey(encodedPublicKey);
                        if (publicKey == null) {
                            logger.error("Uninitialized crypto environment");
                        }
                    }
                }
            }
        }
        return publicKey;
    }
}

