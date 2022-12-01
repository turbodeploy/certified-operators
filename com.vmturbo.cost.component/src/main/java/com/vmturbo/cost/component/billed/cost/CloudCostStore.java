package com.vmturbo.cost.component.billed.cost;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostQuery;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStat;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStatsQuery;
import com.vmturbo.components.common.diagnostics.MultiStoreDiagnosable;

/**
 * A store for {@link BilledCostData}.
 */
public interface CloudCostStore extends MultiStoreDiagnosable, CloudCostDiags {

    /**
     * Creates a new {@link BilledCostPersistenceSession}. The persistence sessions represents a queue
     * of cost data to be persisted as part of an upload of cost data.
     * @return The persistence session.
     */
    @Nonnull
    BilledCostPersistenceSession createPersistenceSession();

    /**
     * Stores the provided {@link BilledCostData}.
     * @param costData The cost data to persist.
     * @throws Exception An exception in persisting the cost data.
     */
    void storeCostData(@Nonnull BilledCostData costData) throws Exception;

    /**
     * Gets cost data matching the provided {@code billedCostQuery}.
     * @param billedCostQuery The cost query.
     * @return The list of {@link BilledCostData} matching the query.
     */
    @Nonnull
    List<BilledCostData> getCostData(@Nonnull BilledCostQuery billedCostQuery);

    /**
     * Gets cost stats based on the provided query.
     * @param costStatsQuery The cloud cost stats query.
     * @return The costs stats, sorted by sample timestamp.
     */
    @Nonnull
    List<BilledCostStat> getCostStats(@Nonnull BilledCostStatsQuery costStatsQuery);

    /**
     * A cost persistence session, useful as a short-term queue to process batches of
     * cost data as part of a larger persistence operation.
     */
    interface BilledCostPersistenceSession {

        /**
         * Stores the provided {@link BilledCostData} asynchronously.
         * @param billedCostData The cloud cost data to store.
         */
        void storeCostDataAsync(@Nonnull BilledCostData billedCostData);

        /**
         * Waits for all async operations to complete or for a max timeout limit to be reached. If the
         * timeout is reached, queued and running operations will be canceled, but previously completed
         * operations will not be rolled back.
         * @throws Exception An exception in persisting cost data.
         */
        void commitSession() throws Exception;
    }
}
