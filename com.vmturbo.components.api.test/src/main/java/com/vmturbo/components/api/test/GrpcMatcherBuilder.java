package com.vmturbo.components.api.test;

import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.hamcrest.TypeSafeMatcher;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

/**
 * Intermediate object when creating {@link GrpcRuntimeExceptionMatcher}s or
 * {@link GrpcExceptionMatcher}s to force users to either specify an expected
 * description, or specify that any description will do.
 *
 * @param <T> The matcher to be built - this is one of {@link GrpcRuntimeExceptionMatcher} or
 *           {@link GrpcExceptionMatcher}.
 */
public class GrpcMatcherBuilder<T extends TypeSafeMatcher> {

    private Status.Code expectedCode;

    private Function<GrpcStatusMatcher, T> matcherFactory;

    /**
     * Use {@link GrpcMatcherBuilder#forException(Code)} or
     * {@link GrpcMatcherBuilder#forRuntimeException(Code)} instead.
     */
    private GrpcMatcherBuilder(@Nonnull final Status.Code status,
                       Function<GrpcStatusMatcher, T> matcherFactory) {
        expectedCode = Objects.requireNonNull(status);
        this.matcherFactory = matcherFactory;
    }

    static GrpcMatcherBuilder<GrpcRuntimeExceptionMatcher> forRuntimeException(
            @Nonnull final Status.Code status) {
        return new GrpcMatcherBuilder<>(status, GrpcRuntimeExceptionMatcher::new);
    }

    static GrpcMatcherBuilder<GrpcExceptionMatcher> forException(
            @Nonnull final Status.Code status) {
        return new GrpcMatcherBuilder<>(status, GrpcExceptionMatcher::new);
    }

    /**
     * Demand that the status returned by {@link StatusRuntimeException#getStatus()}
     * contain a non-null description, and that the description contain the provided suffix.
     *
     * @param substr The expected substring.
     * @return The full {@link GrpcRuntimeExceptionMatcher}.
     */
    @Nonnull
    public T descriptionContains(@Nonnull final String substr) {
        return matcherFactory.apply(new GrpcStatusMatcher(expectedCode, Objects.requireNonNull(substr)));
    }

    /**
     * Allow any description (including null) in the status returned by
     *
     * @return The full matcher.
     */
    @Nonnull
    public T anyDescription() {
        return matcherFactory.apply(new GrpcStatusMatcher(expectedCode, null));
    }
}
