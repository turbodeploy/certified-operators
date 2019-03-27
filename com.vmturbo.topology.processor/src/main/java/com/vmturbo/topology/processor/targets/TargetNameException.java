package com.vmturbo.topology.processor.targets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Checked exception for use in {@link Target#computeDisplayName()} to detect when
 * {@link com.vmturbo.crosstier.common.TargetUtil#getTargetId(List, Map, Collection, Function)} fails.
 */
public class TargetNameException extends Exception {
    public TargetNameException() {
    }

    public TargetNameException(final String message) {
        super(message);
    }

    public TargetNameException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public TargetNameException(final Throwable cause) {
        super(cause);
    }

    public TargetNameException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
