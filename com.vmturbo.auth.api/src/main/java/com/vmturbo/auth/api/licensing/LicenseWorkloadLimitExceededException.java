package com.vmturbo.auth.api.licensing;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Represents a condition where active workloads exceed the limit covered by current licenses.
 */
@ResponseStatus(HttpStatus.FORBIDDEN)
public class LicenseWorkloadLimitExceededException extends RuntimeException {

    /**
     * Constructor.
     */
    public LicenseWorkloadLimitExceededException() {
        super("Active workloads exceed the limit covered by current licenses.");
    }
}