package com.vmturbo.components.api.test;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import io.grpc.Status;
import io.grpc.StatusException;

/**
 * Matcher to use when checking status exceptions from local gRPC service calls during testing.
 * The purpose is to make it easier to check code and description together.
 * Sample use:
 *
 * <code>
 * SomeGrpcService service = ...
 * StreamObserver<Response> responseObserver = mock(...);
 * // Suppose the method returns a NOT_FOUND error.
 * service.callMethod(request, responseObserver);
 *
 * ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
 * verify(responseObserver).onError(exceptionCaptor.capture());
 * assertThat(exceptionCaptor.getValue(),
 *     GrpcExceptionMatcher.hasCode(Code.NOT_FOUND).descriptionContains("someId"))
 * </code>
 */
public class GrpcExceptionMatcher extends TypeSafeMatcher<StatusException> {

    private final GrpcStatusMatcher statusMatcher;

    /**
     * Begin building a matcher for the provided code.
     * To finish building, use {@link GrpcMatcherBuilder#descriptionContains} or
     * {@link GrpcMatcherBuilder#anyDescription()}
     *
     * @param status The expected status code.
     * @return A builder to continue building the matcher.
     */
    public static GrpcMatcherBuilder<GrpcExceptionMatcher> hasCode(Status.Code status) {
        return GrpcMatcherBuilder.forException(status);
    }

    GrpcExceptionMatcher(@Nonnull final GrpcStatusMatcher statusMatcher) {
        this.statusMatcher = Objects.requireNonNull(statusMatcher);
    }

    @Override
    protected boolean matchesSafely(final StatusException exception) {
        return statusMatcher.matchesSafely(exception.getStatus());
    }

    @Override
    public void describeTo(Description description) {
        statusMatcher.describeTo(description);
    }
}
