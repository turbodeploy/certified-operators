package com.vmturbo.cost.component.savings;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;

/**
 * Persists audit event logs for diagnostics and drill-down.
 */
public interface AuditLogWriter {
    /**
     * Whether audit log writing is enabled (via config).
     *
     * @return Whether enabled.
     */
    boolean isEnabled();

    /**
     * Write events to disk.
     *
     * @param events Set of events to persist.
     */
    void write(@Nonnull List<SavingsEvent> events);

    /**
     * Deletes all records older than the given timestamp.
     *
     * @param timestamp Epoch millis for timestamp, any older records are removed.
     * @return Count of records deleted.
     */
    int deleteOlderThan(long timestamp);
}
