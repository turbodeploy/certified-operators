package com.vmturbo.auth.api.authorization.jwt;


import java.util.Optional;

import javax.annotation.Nonnull;

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

    /**
     * Retrieve JWT token from current Spring security context
     *
     * @return JWT token if it exists in the current Spring security context
     */
    private static Optional<String> geJwtTokenFromSpringSecurityContext() {
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
                geJwtTokenFromSpringSecurityContext().ifPresent(jwtToken ->
                        metadata.put(SecurityConstant.JWT_METADATA_KEY, jwtToken)
                );
                super.start(responseListener, metadata);
            }
        };
    }
}
