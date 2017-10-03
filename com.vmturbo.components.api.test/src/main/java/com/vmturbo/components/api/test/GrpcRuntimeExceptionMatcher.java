package com.vmturbo.components.api.test;

import java.util.Objects;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Matcher to use when checking status codes from gRPC stub calls.
 * Sample use:
 *
 * <code>
 * expectedException.expect(GrpcRuntimeExceptionMatcher
 *      .hasCode(Code.NOT_FOUND)
 *      .descriptionContains("someId"));
 * // Actually call the method.
 * serviceStub.callMethod(request);
 * </code>
 */
public class GrpcRuntimeExceptionMatcher extends TypeSafeMatcher<StatusRuntimeException>  {

    private final GrpcStatusMatcher statusMatcher;

    /**
     * Begin building a matcher for the provided code.
     * To finish building, use {@link GrpcMatcherBuilder#descriptionContains} or
     * {@link GrpcMatcherBuilder#anyDescription()}
     *
     * @param status The expected status code.
     * @return A builder to continue building the matcher.
     */
    public static GrpcMatcherBuilder<GrpcRuntimeExceptionMatcher> hasCode(Status.Code status) {
        return GrpcMatcherBuilder.forRuntimeException(status);
    }

    GrpcRuntimeExceptionMatcher(GrpcStatusMatcher context) {
        this.statusMatcher = Objects.requireNonNull(context);
    }

    @Override
    protected boolean matchesSafely(final StatusRuntimeException exception) {
        return statusMatcher.matchesSafely(exception.getStatus());
    }

    @Override
    public void describeTo(Description description) {
        statusMatcher.describeTo(description);
    }
}
