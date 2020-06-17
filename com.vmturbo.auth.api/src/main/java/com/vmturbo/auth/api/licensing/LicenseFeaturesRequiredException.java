package com.vmturbo.auth.api.licensing;

import java.util.Collection;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.vmturbo.platform.sdk.common.util.ProbeLicense;

/**
 * Represents a condition where a requested operation is not covered by the existing licenses.
 */
@ResponseStatus(HttpStatus.FORBIDDEN)
public class LicenseFeaturesRequiredException extends RuntimeException {
    // a collection of the features that are required, but were missing
    private Collection<ProbeLicense> featuresRequired;

    public LicenseFeaturesRequiredException(Collection<ProbeLicense> featuresRequired) {
        super(new StringBuilder("Requires an active license with the following features: ")
                .append(featuresRequired.stream()
                        .map(ProbeLicense::getKey)
                        .collect(Collectors.joining(" ")))
                .toString());
        this.featuresRequired = featuresRequired;
    }

    public Collection<ProbeLicense> getFeaturesRequired() {
        return featuresRequired;
    }
}
