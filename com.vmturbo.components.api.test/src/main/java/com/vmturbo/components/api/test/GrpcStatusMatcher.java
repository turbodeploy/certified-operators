package com.vmturbo.components.api.test;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import io.grpc.Status;
import io.grpc.Status.Code;

/**
 * A common matcher for sharing by {@link GrpcExceptionMatcher} and
 * {@link GrpcRuntimeExceptionMatcher}.
 * <p>
 * This is currently package-private because there's no use case for dealing with {@link Status}
 * directly when testing gRPC services - the {@link Status} is always a part of some exception.
 * Users should create the appropriate exception matcher instead.
 */
class GrpcStatusMatcher extends TypeSafeMatcher<Status> {

    private final Status.Code expectedCode;

    private final Optional<String> expectedDescriptionSubstr;

    private Status.Code foundCode;

    private Optional<String> foundDescription;

    GrpcStatusMatcher(@Nonnull final Code expectedCode,
                      @Nullable final String expectedDescription) {
        this.expectedCode = Objects.requireNonNull(expectedCode);
        this.expectedDescriptionSubstr = Optional.ofNullable(expectedDescription);
    }

    @Override
    protected boolean matchesSafely(final Status status) {
        foundCode = status.getCode();
        foundDescription = Optional.ofNullable(status.getDescription());
        return foundCode == expectedCode && descriptionMatches(foundDescription);
    }

    @Override
    public void describeTo(final Description description) {
        if (foundCode != expectedCode) {
            description.appendValue(foundCode)
                    .appendText(" was found instead of ")
                    .appendValue(expectedCode);
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
    }

    private boolean descriptionMatches(Optional<String> other) {
        return expectedDescriptionSubstr
                .map(expected -> other
                        .map(got -> got.contains(expected))
                        .orElse(false))
                .orElse(true);
    }
}
