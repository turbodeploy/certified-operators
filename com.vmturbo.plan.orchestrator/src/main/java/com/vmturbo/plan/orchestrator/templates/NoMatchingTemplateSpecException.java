package com.vmturbo.plan.orchestrator.templates;

/**
 * Throw exception when template instance can not find matched template spec.
 */
public class NoMatchingTemplateSpecException extends Exception {
    public NoMatchingTemplateSpecException (String message) {
        super(message);
    }
}