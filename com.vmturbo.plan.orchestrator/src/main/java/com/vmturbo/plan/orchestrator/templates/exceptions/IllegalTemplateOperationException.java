package com.vmturbo.plan.orchestrator.templates.exceptions;

import javax.annotation.Nonnull;

/**
 * Exception thrown if there are errors when a user tries to add, modify or delete templates.
 */
public class IllegalTemplateOperationException extends Exception {

    public IllegalTemplateOperationException(final long templateId,
                                             @Nonnull final String reason) {
        super("Illegal template operation! " + templateId + "!. Reason: " + reason);
    }

    public IllegalTemplateOperationException(@Nonnull final String reason) {
        super(reason);
    }
}
