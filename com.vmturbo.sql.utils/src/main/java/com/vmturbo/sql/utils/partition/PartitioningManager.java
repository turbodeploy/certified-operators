package com.vmturbo.sql.utils.partition;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.threeten.extra.PeriodDuration;

import com.vmturbo.components.common.utils.RollupTimeFrame;

/**
 * General implementation of partitioning management.
 */
public class PartitioningManager implements IPartitioningManager {

    private static final Logger logger = LogManager.getLogger();

    protected final DSLContext dsl;
    private final String schemaName;
    private final PartitionsManager partitionsManager;
    private final RetentionSettings retentionSettings;
    private final Map<RollupTimeFrame, PeriodDuration> partitionSizes;
    private final Function<String, Optional<RollupTimeFrame>> tableTimeframeFn;
    private boolean initialRefreshComplete = false;

    /**
     * Create a new instance.
     *
     * @param dsl               {@link DSLContext} for DB access
     * @param schemaName        name of schema whose partitions need to be managed
     * @param partitionsModel   in-memory model of the schema's partitioning information, with
     *                          pass-through operations to create and drop partitions as required
     * @param retentionSettings retention settings that determine when partitions need to be
     *                          dropped
     * @param partitionSizes    per-timeframe target sizes of partitions to be created. Any given
     *                          partition may be sized differently due to pre-existing partitions
     *                          not created with the same configuration or logic, but eventually,
     *                          partitions will be uniformly sized if target size remains unchanged
     * @param tableTimeframeFn  function to determine the timeframe associated with a given table
     * @throws PartitionProcessingException if there's a problem creating this instance (due to the
     *                                      partition scan performed during construction)
     */
    public PartitioningManager(DSLContext dsl, String schemaName,
            PartitionsManager partitionsModel, RetentionSettings retentionSettings,
            Map<RollupTimeFrame, String> partitionSizes,
            Function<String, Optional<RollupTimeFrame>> tableTimeframeFn)
            throws PartitionProcessingException {
        this.dsl = dsl;
        this.schemaName = schemaName;
        this.partitionsManager = partitionsModel;
        this.retentionSettings = retentionSettings;
        this.partitionSizes = parsePartitionSizes(partitionSizes);
        this.tableTimeframeFn = tableTimeframeFn;
        partitionsModel.load(this.schemaName);
    }

    private static Map<RollupTimeFrame, PeriodDuration> parsePartitionSizes(
            Map<RollupTimeFrame, String> unparsed) {
        Map<RollupTimeFrame, PeriodDuration> result = new HashMap<>();
        unparsed.forEach((timeFrame, period) ->
                result.put(timeFrame, PeriodDuration.parse(period)));
        return result;
    }

    @Override
    public void prepareForInsertion(Table<?> table, Timestamp insertionTimestamp)
            throws PartitionProcessingException {
        if (!initialRefreshComplete) {
            refreshPartitionInfo();
        }
        PeriodDuration partitionSize = tableTimeframeFn.apply(table.getName())
                .map(partitionSizes::get)
                .orElse(null);
        try {
            if (partitionSize != null) {
                partitionsManager.ensurePartition(schemaName, table,
                        insertionTimestamp.toInstant(), partitionSize);
            } else {
                throw new PartitionProcessingException(
                        String.format(
                                "Could not determine time frame or partition size for table %s",
                                table.getQualifiedName()));
            }
        } catch (PartitionProcessingException e) {
            throw new PartitionProcessingException(
                    String.format("Failed to ensure partition in table %s for time %s",
                            table, insertionTimestamp), e);
        }
    }

    @Override
    public void performRetentionUpdate() {
        refreshPartitionInfo();
        partitionsManager.dropExpiredPartitions(schemaName, tableTimeframeFn,
                retentionSettings, Instant.now());
    }

    private void refreshPartitionInfo() {
        try {
            partitionsManager.load(schemaName);
        } catch (PartitionProcessingException e) {
            logger.error("Failed to reload partition info for schema {}", schemaName, e);
        }
        while (true) {
            try {
                retentionSettings.refresh();
                this.initialRefreshComplete = true;
                break;
            } catch (Exception e) {
                String msg = "Failed to refresh retention settings"
                        + (initialRefreshComplete ? "" : "; retrying in 10 seconds");
                logger.warn(msg, e);
                if (!initialRefreshComplete) {
                    try {
                        Thread.sleep(10_000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        logger.warn(
                                "Exiting without obtaining retention settings due to interrupt", ex);
                        break;
                    }
                }
            }
        }
    }
}
