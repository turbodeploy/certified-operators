package com.vmturbo.components.api.test;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Matcher to use when checking status codes from gRPC calls.
 * Sample use:
 *
 * <code>
 * expectedException.expect(GrpcExceptionMatcher
 *      .code(Code.NOT_FOUND)
 *      .descriptionContains("someId"));
 * // Actually call the method.
 * serviceStub.callMethod(request);
 * </code>
 */
public class GrpcExceptionMatcher extends TypeSafeMatcher<StatusRuntimeException> {

    private Status.Code foundStatus;

    private final Status.Code expectedStatus;

    private Optional<String> foundDescription;

    private Optional<Class<?>> foundCauseClass;

    private final Optional<String> expectedDescriptionSubstr;

    private final Optional<Class<?>> expectedCauseClass;

    /**
     * Begin building a matcher for the provided code.
     * To finish building, use {@link MatcherBuilder#descriptionContains} or
     * {@link MatcherBuilder#anyDescriptionOrCause()}
     *
     * @param status The expected status code.
     * @return A builder to continue building the matcher.
     */
    public static MatcherBuilder code(Status.Code status) {
        return new MatcherBuilder(status);
    }

    private GrpcExceptionMatcher(@Nonnull final Status.Code expectedStatus,
                                 @Nullable final String expectedDescriptionSubstr,
                                 @Nullable final Class<?> expectedCauseClass) {
        this.expectedStatus = Objects.requireNonNull(expectedStatus);
        this.expectedDescriptionSubstr = Optional.ofNullable(expectedDescriptionSubstr);
        this.expectedCauseClass = Optional.ofNullable(expectedCauseClass);
    }

    @Override
    protected boolean matchesSafely(final StatusRuntimeException exception) {
        foundStatus = exception.getStatus().getCode();
        foundDescription = Optional.ofNullable(exception.getStatus().getDescription());
        foundCauseClass = Optional.ofNullable(exception.getStatus().getCause()).map(Object::getClass);
        return foundStatus == expectedStatus && descriptionMatches(foundDescription);
    }

    @Override
    public void describeTo(Description description) {
        if (foundStatus != expectedStatus) {
            description.appendValue(foundStatus)
                    .appendText(" was found instead of ")
                    .appendValue(expectedStatus);
        }

        // If description doesn't match, expected description must be set.
        // However, make sure expected description is set just so potential
        // bugs in descriptionMatches() don't result in NPE's.
        if (!descriptionMatches(foundDescription) && expectedDescriptionSubstr.isPresent()) {
            if (foundDescription.isPresent()) {
                description.appendValue(expectedDescriptionSubstr.get())
                        .appendText(" was not found in ")
                        .appendValue(foundDescription.get());
            } else {
                description.appendText("No description found. Expected a description containing: ")
                        .appendValue(expectedDescriptionSubstr.get());
            }
        }

        if (!causeMatches(foundCauseClass)) {
            if (foundCauseClass.isPresent()) {
                description.appendValue(expectedCauseClass.get())
                    .appendText(" does not match actual cause class ")
                    .appendValue(foundCauseClass.get());
            } else {
                description.appendText("No cause found. Expected a cause of class: ")
                    .appendValue(expectedCauseClass.get());
            }
        }
    }

    private boolean descriptionMatches(Optional<String> other) {
        return expectedDescriptionSubstr
                .map(expected -> other
                        .map(got -> got.contains(expected))
                        .orElse(false))
                .orElse(true);
    }

    private boolean causeMatches(Optional<Class<?>> actualCauseClass) {
        return expectedCauseClass
            .map(expected -> actualCauseClass
                .map(got -> got.equals(expected))
                .orElse(false))
            .orElse(true);
    }

    /**
     * Intermediate object to force users to either specify an expected
     * description, or specify that any description will do.
     */
    public static class MatcherBuilder {

        private Status.Code expectedCode;

        private MatcherBuilder(@Nonnull final Status.Code status) {
            expectedCode = Objects.requireNonNull(status);
        }

        /**
         * Demand that the status returned by {@link StatusRuntimeException#getStatus()}
         * contain a non-null description, and that the description contain the provided suffix.
         *
         * @param substr The expected substring.
         * @return The full {@link GrpcExceptionMatcher}.
         */
        @Nonnull
        public GrpcExceptionMatcher descriptionContains(@Nonnull final String substr) {
            return new GrpcExceptionMatcher(expectedCode, Objects.requireNonNull(substr), null);
        }

        @Nonnull
        public GrpcExceptionMatcher withCause(@Nonnull final Class<?> expectedCauseClass) {
            return new GrpcExceptionMatcher(expectedCode, null, Objects.requireNonNull(expectedCauseClass));
        }

        /**
         * Allow any description (including null) in the status returned by
         * {@link StatusRuntimeException#getStatus()}.
         *
         * @return The full {@link GrpcExceptionMatcher}.
         */
        @Nonnull
        public GrpcExceptionMatcher anyDescriptionOrCause() {
            return new GrpcExceptionMatcher(expectedCode, null, null);
        }
    }

}
