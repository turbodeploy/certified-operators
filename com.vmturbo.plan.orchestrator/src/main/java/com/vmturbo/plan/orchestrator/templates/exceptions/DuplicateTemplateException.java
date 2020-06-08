package com.vmturbo.plan.orchestrator.templates.exceptions;

import java.text.MessageFormat;

import javax.annotation.Nonnull;

/**
 * Exception thrown if there are errors when a user tries to create templates.
 */
public class DuplicateTemplateException extends Exception {

    public DuplicateTemplateException(@Nonnull final String templateName) {
        super(MessageFormat.format("Template with the name {0} already exists", templateName));
    }
}
