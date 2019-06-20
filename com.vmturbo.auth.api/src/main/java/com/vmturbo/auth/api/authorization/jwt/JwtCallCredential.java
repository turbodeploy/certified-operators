package com.vmturbo.auth.api.authorization.jwt;

import java.util.concurrent.Executor;

import javax.annotation.Nonnull;

import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * Carries JWT token that will be propagated to
 * the server via request metadata for each RPC.
 */
public class JwtCallCredential implements CallCredentials {

    private final String jwt;

    /**
     * Construct call credential with JWT token.
     * @param jwt user's JWT token
     */
    public JwtCallCredential(@Nonnull String jwt) {
        this.jwt = jwt;
    }

    /**
     * Pass the JWT token to the given {@link MetadataApplier}, which will propagate it to
     * the request metadata.
     *
     * @param method The method descriptor of this RPC
     * @param attrs Additional attributes from the transport, along with the keys defined in this
     *        interface (i.e. the {@code ATTR_*} fields) which are guaranteed to be present.
     * @param appExecutor The application thread-pool. It is provided to the implementation in case it
     *        needs to perform blocking operations.
     * @param applier The outlet of the produced headers. It can be called either before or after this
     *        method returns.
     */
    @Override
    public void applyRequestMetadata(@Nonnull MethodDescriptor<?, ?> method,
                                     @Nonnull Attributes attrs,
                                     @Nonnull Executor appExecutor,
                                     @Nonnull MetadataApplier applier) {
        appExecutor.execute(() -> {
            try {
                Metadata headers = new Metadata();
                Metadata.Key<String> jwtKey = Metadata.Key.of("jwt", Metadata.ASCII_STRING_MARSHALLER);
                headers.put(jwtKey, jwt);
                applier.apply(headers);
            } catch (Throwable e) {
                applier.fail(Status.UNAUTHENTICATED.withCause(e));
            }
        });
    }

    @Override
    public void thisUsesUnstableApi() {
        // Just a method to indicate that the underlying CallCredentials interface
        // is experimental/unsable.
    }
}
