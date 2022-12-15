package com.vmturbo.cost.component.scope;

import java.time.Instant;
import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Log to store scope id replacements.
 */
public interface ScopeIdReplacementLog {

    /**
     * Persists scope id replacements and returns the summary.
     *
     * @return summary of scope id replacements.
     * @throws Exception if error encountered during persistence.
     */
    Optional<ScopeIdReplacementPersistenceSummary> persistScopeIdReplacements() throws Exception;

    /**
     * Returns a replaced scopeId for the provided scopeId and sampleTimeUtcMs. If multiple replacements have been made
     * for the provided scopeId, it uses the provided sampleTimeUtcMs to find the appropriate replacement: the first
     * replacement that has a firstDiscoveredTimeMsUtc earlier than the provided sampleTimeUtcMs.
     *
     * @param scopeId for which the replaced scopeId is being retrieved.
     * @param sampleTimeUtcMs for which the scopeId is being retrieved.
     * @return replaced scopeId.
     */
    long getReplacedScopeId(long scopeId, @Nonnull Instant sampleTimeUtcMs);

    /**
     * Returns the unique id of the {@link ScopeIdReplacementLog} singleton used to persist records for this log.
     *
     * @return unique
     */
    short getLogId();
}