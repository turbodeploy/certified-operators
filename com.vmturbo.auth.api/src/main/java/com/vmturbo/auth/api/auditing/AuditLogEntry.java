package com.vmturbo.auth.api.auditing;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Builder for audit entry. It should be used with {@code AuditLogUtils#audit(AuditLogEntry)}
 * as demonstrated below:
 * <blockquote><pre>
 * AuditLogEntry entry = new AuditLogEntry.Builder(action, detailMessage, false)
 *                       .actionInitiator(actionInitiator)
 *                       .targetName(targetName)
 *                       .remoteClientIP(remoteClientIP)
 *                       .build();
 * AuditLogUtils.audit(entry);
 * </pre></blockquote>
 */
public class AuditLogEntry {

    /**
     * The current action.
     */
    private AuditAction action;

    /**
     * The user or system that initialized the action.
     */
    private String actionInitiator;

    /**
     * The target object name the action apply to.
     */
    private String targetName;

    /**
     * The detail message.
     */
    private String detailMessage;

    /**
     * Remote IP address performing the action.
     */
    private String remoteClientIP;

    /**
     * Is the action is successful.
     */
    private boolean isSuccessful;

    public AuditAction getAction() {
        return action;
    }

    public String getActionInitiator() {
        return actionInitiator;
    }

    public String getTargetName() {
        return targetName;
    }

    public String getDetailMessage() {
        return detailMessage;
    }

    public String getRemoteClientIP() {
        return remoteClientIP;
    }

    public boolean isSuccessful() {
        return isSuccessful;
    }

    /**
     * Builder for audit log entry.
     */
    public static class Builder {

        // Required parameters
        private AuditAction action;
        private String detailMessage;
        private boolean isSuccessful;

        // Optional parameters
        private String actionInitiator;
        private String targetName;
        private String remoteClientIP;

        /**
         * The Builder constructor.
         *
         * @param action action The current action.
         * @param detailMessage The detail message.
         * @param isSuccessful true if the action is successful
         */
        public Builder(@Nonnull final AuditAction action, @Nonnull final String detailMessage,
                        final boolean isSuccessful) {
            this.action = Objects.requireNonNull(action);
            this.detailMessage = Objects.requireNonNull(detailMessage);
            this.isSuccessful = isSuccessful;
        }

        /**
         * Set the action initiator.
         *
         * @param actionInitiator The user or system that initialized the action.
         * @return the builder
         */
        public Builder actionInitiator(final String actionInitiator) {
            this.actionInitiator = actionInitiator;
            return this;
        }

        /**
         * Set the target name.
         *
         * @param targetName The target object the action apply to.
         * @return the builder
         */
        public Builder targetName(final String targetName) {
            this.targetName = targetName;
            return this;
        }

        /**
         * Set the remote IP address.
         *
         * @param remoteClientIP remote IP address performing the action.
         * @return the builder
         */
        public Builder remoteClientIP(final String remoteClientIP) {
            this.remoteClientIP = remoteClientIP;
            return this;
        }

        /**
         * Return the AuditLogEntry.
         *
         * @return the AuditLogEntry.
         */
        public AuditLogEntry build() {
            return new AuditLogEntry(this);
        }

        /**
         * Write to audit log
         */
        public void audit() {
            AuditLogUtils.audit(this.build());
        }
    }

    /**
     * Construct the audit log entry.
     *
     * @param builder the AuditLogEntry builder
     */
    private AuditLogEntry(Builder builder) {
        this.action = builder.action;
        this.actionInitiator = builder.actionInitiator;
        this.targetName = builder.targetName;
        this.detailMessage = builder.detailMessage;
        this.remoteClientIP = builder.remoteClientIP;
        this.isSuccessful = builder.isSuccessful;
    }
}
