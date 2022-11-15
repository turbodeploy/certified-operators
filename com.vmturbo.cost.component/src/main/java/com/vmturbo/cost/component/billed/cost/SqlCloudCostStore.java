package com.vmturbo.cost.component.billed.cost;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.cloud.common.persistence.DataQueueFactory;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityProvider;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostQuery;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStat;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStatsQuery;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.sql.utils.partition.IPartitioningManager;

/**
 * SQL implementation of {@link CloudCostStore}.
 */
public class SqlCloudCostStore implements CloudCostStore {

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final BilledCostWriter costWriter;

    private final SqlBilledCostPersistenceSession.Factory persistenceSessionFactory;

    private final TagGroupIdentityService tagGroupIdentityService;

    private final SqlCostStatsQueryExecutor statsQueryExecutor;

    /**
     * Constructs a new {@link SqlCloudCostStore} instance.
     *
     * @param partitioningManager The partitioning manager.
     * @param tagGroupIdentityService Tag group identity service.
     * @param scopeIdentityProvider The scope identity provider.
     * @param dataQueueFactory The data queue factory.
     * @param timeFrameCalculator The stats query executor.
     * @param dsl The DSL context.
     * @param persistenceConfig The cost data persistence config.
     * @param persistenceBatchSize Batch store size.
     */
    public SqlCloudCostStore(@Nonnull IPartitioningManager partitioningManager,
                             @Nonnull TagGroupIdentityService tagGroupIdentityService,
                             @Nonnull CloudScopeIdentityProvider scopeIdentityProvider,
                             @Nonnull DataQueueFactory dataQueueFactory,
                             @Nonnull TimeFrameCalculator timeFrameCalculator,
                             @Nonnull DSLContext dsl,
                             @Nonnull BilledCostPersistenceConfig persistenceConfig,
                             int persistenceBatchSize) {

        this.dsl = Objects.requireNonNull(dsl);
        this.costWriter = new BilledCostWriter(partitioningManager, tagGroupIdentityService, scopeIdentityProvider, dsl, persistenceBatchSize);
        this.tagGroupIdentityService = Objects.requireNonNull(tagGroupIdentityService);
        this.statsQueryExecutor = new SqlCostStatsQueryExecutor(dsl, tagGroupIdentityService, timeFrameCalculator);
        persistenceSessionFactory = new SqlBilledCostPersistenceSession.Factory(
                costWriter,
                dataQueueFactory,
                persistenceConfig);
    }

    @Override
    public BilledCostPersistenceSession createPersistenceSession() {

        return persistenceSessionFactory.createPersistenceSession();
    }

    @Override
    public void storeCostData(@NotNull BilledCostData costData) throws Exception {
        costWriter.persistCostData(costData);
    }

    @Nonnull
    @Override
    public List<BilledCostData> getCostData(@Nonnull BilledCostQuery billedCostQuery) {

        logger.debug("Executing billed cost query: {}", billedCostQuery);

        final BilledCostTableAccessor<?> tableAccessor = resolveTableAccessor(billedCostQuery.getGranularity());

        final BilledCostDataBuilder costDataBuilder = BilledCostDataBuilder.create(
                tableAccessor.granularity(),
                tagGroupIdentityService::getTagGroupsById);
        try (Stream<Record> recordStream = dsl.selectFrom(resolveRequiredTables(tableAccessor, billedCostQuery))
                .where(tableAccessor.filterMapper().generateConditions(billedCostQuery.getFilter()))
                // The BilledCostDataBuilder will maintain this order in adding cloud cost buckets
                // to the corresponding cloud cost data instances.
                .orderBy(tableAccessor.sampleTs())
                .stream()) {

            recordStream.map(tableAccessor::createRecordAccessor)
                    .forEach(costItemAccessor ->
                        costDataBuilder.addCostItem(
                                costItemAccessor.billingFamilyId(),
                                costItemAccessor.serviceProviderId(),
                                costItemAccessor.sampleTs(),
                                costItemAccessor.tagGroupId(),
                                costItemAccessor.toCostItem()));
        }

        return costDataBuilder.buildCostDataList(billedCostQuery.getResolveTagGroups());
    }

    @NotNull
    @Override
    public List<BilledCostStat> getCostStats(@Nonnull BilledCostStatsQuery costStatsQuery) {

        return statsQueryExecutor.getCostStats(costStatsQuery);
    }

    private BilledCostTableAccessor<?> resolveTableAccessor(@Nonnull Granularity granularity) {

        switch (granularity) {
            case HOURLY:
                return BilledCostTableAccessor.CLOUD_COST_HOURLY;
            case DAILY:
                return BilledCostTableAccessor.CLOUD_COST_DAILY;
            default:
                throw new UnsupportedOperationException(
                        String.format("Granularity %s is not supported cost cloud cost", granularity));
        }
    }

    private Table<Record> resolveRequiredTables(@Nonnull BilledCostTableAccessor<?> costTableAccessor,
                                                @Nonnull BilledCostQuery costQuery) {

        Table<Record> aggregateTable = costTableAccessor.table()
                .join(Tables.CLOUD_SCOPE)
                .on(costTableAccessor.scopeId().eq(Tables.CLOUD_SCOPE.SCOPE_ID));

        if (costQuery.getFilter().hasTagFilter()) {
            aggregateTable = aggregateTable.join(Tables.COST_TAG_GROUPING)
                    .on(costTableAccessor.tagGroupId().eq(Tables.COST_TAG_GROUPING.TAG_GROUP_ID))
                    .join(Tables.COST_TAG)
                    .on(Tables.COST_TAG_GROUPING.TAG_ID.eq(Tables.COST_TAG.TAG_ID));
        }

        return aggregateTable;
    }
}
