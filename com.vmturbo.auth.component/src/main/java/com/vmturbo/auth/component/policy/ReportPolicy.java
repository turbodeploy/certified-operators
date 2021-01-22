package com.vmturbo.auth.component.policy;

/**
 * Report related policies.
 */
public class ReportPolicy {
    private final int allowedMaximumEditor;

    /**
     * Constructor.
     *
     * @param allowedMaximumEditor allowed number of maximum editor.
     */
    public ReportPolicy(final int allowedMaximumEditor) {
        this.allowedMaximumEditor = allowedMaximumEditor;
    }

    /**
     * Getter.
     *
     * @return allowed number of maximum editor
     *
     */
    public int getAllowedMaximumEditor() {
        return allowedMaximumEditor;
    }
}
