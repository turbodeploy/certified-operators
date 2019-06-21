package com.vmturbo.auth.api.auditing;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Audit log utility class to support fluent api.
 *
 * Example usage:
 * <p>
 * AuditLog.newEntry(AuditAction.DELETE_USER, details, false)
 *                 .targetName(uuid)
 *                 .audit();
 * </p>
 */
public class AuditLog {

    // Don't allow initialization
    private AuditLog(){
    }

    /**
     * Create an new audit entry builder.
     * @param auditAction audit action
     * @param detailMsg detail message
     * @param isSuccessful is the action successful
     * @return audit entry builder
     */
    public static AuditLogEntry.Builder newEntry(@Nonnull AuditAction auditAction,
                                                 @Nonnull String detailMsg,
                                                 boolean isSuccessful) {
        return new AuditLogEntry.Builder(Objects.requireNonNull(auditAction),
            Objects.requireNonNull(detailMsg), isSuccessful);
    }
}
