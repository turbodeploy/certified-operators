package com.vmturbo.cost.component.savings;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;

/**
 * Persists audit event logs for diagnostics and drill-down.
 */
public interface AuditLogWriter {
    /**
     * Write events to disk.
     *
     * @param events Set of events to persist.
     */
    void write(@Nonnull List<SavingsEvent> events);
}
