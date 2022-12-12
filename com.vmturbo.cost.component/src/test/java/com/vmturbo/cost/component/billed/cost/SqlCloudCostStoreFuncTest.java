package com.vmturbo.cost.component.billed.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.cloud.common.persistence.DataQueueFactory;
import com.vmturbo.cloud.common.persistence.DataQueueFactory.DefaultDataQueueFactory;
import com.vmturbo.cloud.common.scope.CachedAggregateScopeIdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore.PersistenceRetryPolicy;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostFilter;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostQuery;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStat;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStatsQuery;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStatsQuery.BilledCostGroupBy;
import com.vmturbo.common.protobuf.cost.BilledCost.TagFilter;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.billed.cost.CloudCostStore.BilledCostPersistenceSession;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.billedcosts.TagGroupStore;
import com.vmturbo.cost.component.billedcosts.TagIdentityService;
import com.vmturbo.cost.component.billedcosts.TagStore;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.scope.SqlCloudCostScopeIdReplacementLog;
import com.vmturbo.cost.component.scope.SqlCloudScopeIdentityStore;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.sql.utils.partition.IPartitioningManager;

/**
 * Functional tests for {@link SqlCloudCostStore}, including integration with the tag group identity provider and
 * cloud scope identity provider.
 */
@RunWith(Parameterized.class)
public class SqlCloudCostStoreFuncTest extends MultiDbTestBase {

    private static final BilledCostPersistenceConfig DEFAULT_PERSISTENCE_CONFIG = BilledCostPersistenceConfig.builder()
            .concurrency(2)
            .persistenceTimeout(Duration.ofSeconds(30))
            .build();

    private static final String COST_DATA_BASE_RESOURCE_PATH = "/cloud/cost/cloud_cost_base.json";

    /**
     * Provide test parameters. Note that currently the cloud cost store does not support partition
     * creation for Postgres.
     * @return test parameters.
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
    }

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private CloudScopeIdentityProvider cloudScopeIdentityProvider;

    private TagGroupIdentityService tagGroupIdentityService;

    private TagGroupStore tagGroupStore;

    private DataQueueFactory dataQueueFactory;

    private TimeFrameCalculator timeFrameCalculator;

    private SqlCloudCostScopeIdReplacementLog sqlCloudCostScopeIdReplacementLog;


    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SqlCloudCostStoreFuncTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Set up.
     * @throws InitializationException Initialization exception.
     */
    @Before
    public void setup() throws InitializationException {

        // set up the cloud scope identity provider
        final CloudScopeIdentityStore scopeIdentityStore = new SqlCloudScopeIdentityStore(
                dsl, PersistenceRetryPolicy.DEFAULT_POLICY, false, 10);
        final IdentityProvider identityProvider = new DefaultIdentityProvider(0);

        cloudScopeIdentityProvider = new CachedAggregateScopeIdentityProvider(
                scopeIdentityStore, identityProvider, Duration.ofSeconds(10));
        cloudScopeIdentityProvider.initialize();

        // set up the tag store and identity service
        final TagStore tagStore = new TagStore(dsl);
        final TagIdentityService tagIdentityService = new TagIdentityService(tagStore, identityProvider, 10);
        tagGroupStore = new TagGroupStore(dsl);
        tagGroupIdentityService = new TagGroupIdentityService(tagGroupStore, tagIdentityService, identityProvider, 10);

        dataQueueFactory = new DefaultDataQueueFactory();

        //set up the time frame calculator
        timeFrameCalculator = mock(TimeFrameCalculator.class);
        when(timeFrameCalculator.millis2TimeFrame(anyLong())).thenReturn(TimeFrame.HOUR);

        sqlCloudCostScopeIdReplacementLog = mock(SqlCloudCostScopeIdReplacementLog.class);
        when(sqlCloudCostScopeIdReplacementLog.getReplacedScopeId(anyLong(), any(Instant.class)))
            .thenAnswer(i -> i.getArguments()[0]);
    }

    /**
     * Tests synchronous persistence and querying of {@link BilledCostData}.
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testSynchronousPersistence() throws Exception {

        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData(COST_DATA_BASE_RESOURCE_PATH);
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostData> retrievedCostDataList = cloudCostStore.getCostData(BilledCostQuery.newBuilder()
                .setResolveTagGroups(true)
                .build());

        /*
        ASSERTIONS
         */
        assertThat(retrievedCostDataList, hasSize(1));
        final BilledCostData retrievedCostData = retrievedCostDataList.get(0);
        compareCostData(retrievedCostData, persistedCostData);
    }

    /**
     * Tests async persistence and querying of {@link BilledCostData}.
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testAsyncPersistence() throws Exception {

        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData(COST_DATA_BASE_RESOURCE_PATH);
        final BilledCostPersistenceSession persistenceSession = cloudCostStore.createPersistenceSession();
        persistenceSession.storeCostDataAsync(persistedCostData);
        persistenceSession.commitSession();

        final List<BilledCostData> retrievedCostDataList = cloudCostStore.getCostData(BilledCostQuery.newBuilder()
                .setResolveTagGroups(true)
                .build());

        /*
        ASSERTIONS
         */
        assertThat(retrievedCostDataList, hasSize(1));
        final BilledCostData retrievedCostData = retrievedCostDataList.get(0);
        compareCostData(retrievedCostData, persistedCostData);
    }

    /**
     * Test that {@link CloudCostStore#storeCostData(BilledCostData)} looks up
     * {@link SqlCloudCostScopeIdReplacementLog#getReplacedScopeId(long, Instant)} for swapping out an alias scope_id
     * with a real scope_id.
     *
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testPersistenceWithScopeIdReplacement() throws Exception {
        final long aliasOid = 1L;
        final long realOid = 2L;
        when(sqlCloudCostScopeIdReplacementLog.getReplacedScopeId(eq(aliasOid), any(Instant.class)))
            .thenReturn(realOid);

        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);
        final BilledCostData persistedCostData = BilledCostData.newBuilder()
            .setGranularity(Granularity.DAILY)
            .addCostBuckets(BilledCostBucket.newBuilder()
                .addCostItems(BilledCostItem.newBuilder()
                    .setEntityId(aliasOid)
                    .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                    .build())
                .build())
            .build();

        cloudCostStore.storeCostData(persistedCostData);

        Assert.assertTrue(dsl.selectFrom(Tables.CLOUD_COST_DAILY)
            .where(Tables.CLOUD_COST_DAILY.SCOPE_ID.eq(realOid))
            .fetch().isNotEmpty());
    }

    /**
     * Tests cost stags grouping by account.
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testAccountGroupByStats() throws Exception {

        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData("/cloud/cost/cost_group_by_account_region.json");
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostStat> retrievedCostDataList = cloudCostStore.getCostStats(BilledCostStatsQuery.newBuilder()
                .setGranularity(Granularity.HOURLY)
                .addGroupBy(BilledCostGroupBy.ACCOUNT)
                .build());

        final List<BilledCostStat> expectedStats = loadStatsList("/cloud/cost/expected/group_by_account_list.json");
        assertThat(retrievedCostDataList, hasSize(expectedStats.size()));
        assertThat(retrievedCostDataList, containsInAnyOrder(expectedStats.toArray(new BilledCostStat[0])));
    }

    /**
     * Tests cost stats grouping by both account and region.
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testAccountRegionGroupByStats() throws Exception {

        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData("/cloud/cost/cost_group_by_account_region.json");
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostStat> retrievedCostDataList = cloudCostStore.getCostStats(BilledCostStatsQuery.newBuilder()
                .setGranularity(Granularity.HOURLY)
                .addGroupBy(BilledCostGroupBy.ACCOUNT)
                .addGroupBy(BilledCostGroupBy.REGION)
                .build());

        final List<BilledCostStat> expectedStats = loadStatsList("/cloud/cost/expected/group_by_account_region_list.json");
        assertThat(retrievedCostDataList, hasSize(expectedStats.size()));
        assertThat(retrievedCostDataList, containsInAnyOrder(expectedStats.toArray(new BilledCostStat[0])));
    }

    /**
     * Tests that a validation exception is thrown if a request for group by tag does not include
     * a tag filter.
     * @throws Exception Unexpected exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidGroupByTag() throws Exception {
        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData("/cloud/cost/cloud_cost_tags.json");
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostStat> retrievedCostDataList = cloudCostStore.getCostStats(BilledCostStatsQuery.newBuilder()
                .setGranularity(Granularity.HOURLY)
                .addGroupBy(BilledCostGroupBy.TAG)
                .build());
    }

    /**
     * Tests group by tag with a tag key filter. Verifies that tag keys and values are trimmed prior to
     * persisting to the tag store.
     * @throws Exception Exception loading cost data.
     */
    @Test
    public void testGroupByTagKeyFilter() throws Exception {
        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData("/cloud/cost/cloud_cost_tags.json");
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostStat> retrievedCostDataList = cloudCostStore.getCostStats(BilledCostStatsQuery.newBuilder()
                .setGranularity(Granularity.HOURLY)
                        .setFilter(BilledCostFilter.newBuilder()
                                .setTagFilter(TagFilter.newBuilder()
                                        .setTagKey("tagKey")))
                .addGroupBy(BilledCostGroupBy.TAG)
                .build());

        final List<BilledCostStat> expectedStats = loadStatsList("/cloud/cost/expected/group_by_tag_key_filter.json");
        assertThat(retrievedCostDataList, hasSize(expectedStats.size()));
        assertThat(retrievedCostDataList, containsInAnyOrder(expectedStats.toArray(new BilledCostStat[0])));
    }

    /**
     * Tests a group by tag query with a tag filter for both tag key and value.
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testGroupByTagValueFilter() throws Exception {
        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData("/cloud/cost/cloud_cost_tags.json");
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostStat> retrievedCostDataList = cloudCostStore.getCostStats(BilledCostStatsQuery.newBuilder()
                .setGranularity(Granularity.HOURLY)
                .setFilter(BilledCostFilter.newBuilder()
                        .setTagFilter(TagFilter.newBuilder()
                                .setTagKey("tagKey")
                                .addTagValue("tagValue1")))
                .addGroupBy(BilledCostGroupBy.TAG)
                .build());

        final List<BilledCostStat> expectedStats = loadStatsList("/cloud/cost/expected/group_by_tag_value_filter.json");
        assertThat(retrievedCostDataList, hasSize(expectedStats.size()));
        assertThat(retrievedCostDataList, containsInAnyOrder(expectedStats.toArray(new BilledCostStat[0])));
    }

    /**
     * Tests grouping by tag group.
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testGroupByTagGroup() throws Exception {
        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData("/cloud/cost/cloud_cost_tags.json");
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostStat> retrievedCostDataList = cloudCostStore.getCostStats(BilledCostStatsQuery.newBuilder()
                .setGranularity(Granularity.HOURLY)
                .addGroupBy(BilledCostGroupBy.TAG_GROUP)
                .build());

        final List<BilledCostStat> expectedStats = loadStatsList("/cloud/cost/expected/group_by_tag_group.json");
        assertThat(retrievedCostDataList, hasSize(expectedStats.size()));
        assertThat(retrievedCostDataList, containsInAnyOrder(expectedStats.toArray(new BilledCostStat[0])));
    }

    /**
     * Test group by cost category and price model.
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testGroupByPriceModelAndCategory() throws Exception {
        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData("/cloud/cost/cloud_cost_base.json");
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostStat> retrievedCostDataList = cloudCostStore.getCostStats(BilledCostStatsQuery.newBuilder()
                .setGranularity(Granularity.HOURLY)
                .addGroupBy(BilledCostGroupBy.COST_CATEGORY)
                .addGroupBy(BilledCostGroupBy.PRICE_MODEL)
                .build());

        final List<BilledCostStat> expectedStats = loadStatsList("/cloud/cost/expected/group_by_price_model_category.json");
        assertThat(retrievedCostDataList, hasSize(expectedStats.size()));
        assertThat(retrievedCostDataList, containsInAnyOrder(expectedStats.toArray(new BilledCostStat[0])));

    }

    /**
     * Tests grouping by tag group with a tag key filter.
     * @throws Exception Unexpected exception.
     */
    @Test
    public void testGroupByTagGroupKeyFilter() throws Exception {
        final SqlCloudCostStore cloudCostStore = createCostStore(DEFAULT_PERSISTENCE_CONFIG);

        final BilledCostData persistedCostData = loadCostData("/cloud/cost/cloud_cost_tags.json");
        cloudCostStore.storeCostData(persistedCostData);

        final List<BilledCostStat> retrievedCostDataList = cloudCostStore.getCostStats(BilledCostStatsQuery.newBuilder()
                .setGranularity(Granularity.HOURLY)
                .setFilter(BilledCostFilter.newBuilder()
                        .setTagFilter(TagFilter.newBuilder()
                                .setTagKey("tagKey2")))
                .addGroupBy(BilledCostGroupBy.TAG_GROUP)
                .build());

        final List<BilledCostStat> expectedStats = loadStatsList("/cloud/cost/expected/group_by_tag_group_key_filter.json");
        assertThat(retrievedCostDataList, hasSize(expectedStats.size()));
        assertThat(retrievedCostDataList, containsInAnyOrder(expectedStats.toArray(new BilledCostStat[0])));
    }

    private SqlCloudCostStore createCostStore(@Nonnull BilledCostPersistenceConfig persistenceConfig) {

        final IPartitioningManager partitioningManager = mock(IPartitioningManager.class);

        return new SqlCloudCostStore(partitioningManager,
                tagGroupIdentityService,
                cloudScopeIdentityProvider,
                sqlCloudCostScopeIdReplacementLog,
                dataQueueFactory,
                timeFrameCalculator,
                dsl,
                persistenceConfig);
    }

    private BilledCostData loadCostData(@Nonnull String costDataProtoFilepath) throws Exception {

        final BilledCostData.Builder costData = BilledCostData.newBuilder();
        try (InputStream is = SqlCloudCostStoreFuncTest.class.getResourceAsStream(costDataProtoFilepath);
             InputStreamReader isr = new InputStreamReader(is)) {

            JsonFormat.parser().merge(isr, costData);
        }

        return costData.build();
    }

    private List<BilledCostStat> loadStatsList(@Nonnull String costDataProtoFilepath) throws IOException {

        final ImmutableList.Builder<BilledCostStat> costStatsList = ImmutableList.builder();
        try (InputStream is = SqlCloudCostStoreFuncTest.class.getResourceAsStream(costDataProtoFilepath)) {

            final ObjectMapper objectMapper = new ObjectMapper();
            final String statsString = IOUtils.toString(is);
            final List<JsonNode> statsNodeList = objectMapper.readValue(statsString, new TypeReference<List<JsonNode>>(){});

            for (JsonNode statNode : statsNodeList) {
                final BilledCostStat.Builder costStat = BilledCostStat.newBuilder();
                JsonFormat.parser().merge(statNode.toString(), costStat);
                costStatsList.add(costStat.build());
            }
        }

        return costStatsList.build();
    }

    private void compareCostData(@Nonnull BilledCostData actualCostData,
                                 @Nonnull BilledCostData expectedCostData) {

        assertThat(actualCostData.getCostBucketsCount(), equalTo(expectedCostData.getCostBucketsCount()));

        // check that tag group maps are identical
        assertThat(actualCostData.getCostTagGroupCount(), equalTo(expectedCostData.getCostTagGroupCount()));
        assertThat(actualCostData.getCostTagGroupMap().values(),
                containsInAnyOrder(expectedCostData.getCostTagGroupMap().values().toArray(new CostTagGroup[0])));

        // Assume expected buckets are sorted by timestamp. Actual buckets should be sorted
        for (int i = 0; i < actualCostData.getCostBucketsCount(); i++) {

            final BilledCostBucket actualBucket = actualCostData.getCostBuckets(i);
            final BilledCostBucket expectedBucket = expectedCostData.getCostBuckets(i);

            assertThat(actualBucket.getCostItemsCount(), equalTo(expectedBucket.getCostItemsCount()));

            assertThat(clearInfoForComparison(actualBucket.getCostItemsList()),
                    containsInAnyOrder(clearInfoForComparison(expectedBucket.getCostItemsList()).toArray(new BilledCostItem[0])));
        }
    }

    private List<BilledCostItem> clearInfoForComparison(@Nonnull List<BilledCostItem> costItems) {
        return costItems.stream()
                .map(costItem -> costItem.toBuilder().clearCostTagGroupId().build())
                .collect(ImmutableList.toImmutableList());
    }
}