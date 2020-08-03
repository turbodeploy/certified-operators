package com.vmturbo.auth.api.licensing;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * A runtime exception representing a state where the {@link LicenseCheckClient} is not ready yet.
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
public class LicenseCheckNotReadyException extends RuntimeException {
}
