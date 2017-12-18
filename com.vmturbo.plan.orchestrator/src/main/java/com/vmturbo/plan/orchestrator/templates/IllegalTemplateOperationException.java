package com.vmturbo.plan.orchestrator.templates;

import javax.annotation.Nonnull;

/**
 * Exception thrown if there are errors when a user tries to modify/delete templates.
 */
public class IllegalTemplateOperationException extends Exception {

    public IllegalTemplateOperationException(final long templateId,
                                             @Nonnull final String reason) {
        super("Illegal attempt to change/delete template " + templateId + "!. Reason: " + reason);
    }
}
