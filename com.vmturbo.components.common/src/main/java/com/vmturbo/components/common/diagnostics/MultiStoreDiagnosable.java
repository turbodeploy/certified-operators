package com.vmturbo.components.common.diagnostics;

import java.util.Set;

/**
 * Interface used to save diagnostic dumps for multiple tables relevant to a single store. For example,
 * the entity cost store can queries the current, hourly, daily and monthly tables.
 */
public interface MultiStoreDiagnosable {
    /**
     * Get a list of diagnosables relevant to the particular data stores.
     *
     * @param collectHistoricalStats true if we want to collect diags from rolled up dbs.
     *
     * @return A set of diagnosables.
     */
    Set<Diagnosable> getDiagnosables(boolean collectHistoricalStats);
}
