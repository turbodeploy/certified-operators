package com.vmturbo.auth.api.authorization.jwt;


import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 * Intercept client call and add JWT token to the metadata.
 */
public class JwtClientInterceptor implements ClientInterceptor {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Retrieve JWT token from current Spring security context, or the GRPC Context, if it exists
     * in either place. The GRPC Context is checked first.
     *
     * @return JWT token, if found. Empty optional otherwise.
     */
    private static Optional<String> getJwtTokenFromSecurityContext() {
        // First check if we have a JWT in the grpc context.
        String grpcJwt = SecurityConstant.CONTEXT_JWT_KEY.get();
        if (StringUtils.isNotEmpty(grpcJwt)) {
            return Optional.of(grpcJwt);
        }

        // Nope -- now check the spring security context.
        return SAMLUserUtils
                .getAuthUserDTO()
                .map(AuthUserDTO::getToken);
    }

    /**
     * Intercept {@link ClientCall} creation by the {@code next} {@link Channel},
     * and add JWT token to the metadata.
     *
     * @param method      the remote method to be called.
     * @param callOptions the runtime options to be applied to this call.
     * @param next        the channel which is being intercepted.
     * @return the call object for the remote operation, never {@code null}.
     */
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(@Nonnull MethodDescriptor<ReqT, RespT> method,
                                                               @Nonnull CallOptions callOptions,
                                                               @Nonnull Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata metadata) {
                Optional<String> jwtToken = getJwtTokenFromSecurityContext();
                if (jwtToken.isPresent()) {
                    metadata.put(SecurityConstant.JWT_METADATA_KEY, jwtToken.get());
                    logger.debug("Added JWT Metadata to GRPC Metadata for method {}", method.getFullMethodName());
                } else {
                    logger.trace("No JWT found -- will not add metadata for method {}", method.getFullMethodName());
                }
                super.start(responseListener, metadata);
            }
        };
    }
}
