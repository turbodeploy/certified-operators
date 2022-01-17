package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
import static com.vmturbo.trax.Trax.trax;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.logging.log4j.util.TriConsumer;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost.CostSourceLinkDTO;
import com.vmturbo.common.protobuf.cost.Cost.StatValue;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.persistence.DataIngestionBouncer;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.trax.TraxNumber;

@RunWith(Parameterized.class)
public class SqlEntityCostStoreTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SqlEntityCostStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private static final long ID1 = 1L;
    private static final long ID2 = 2L;
    private static final int ASSOCIATED_ENTITY_TYPE1 = 1;
    private static final int ASSOCIATED_ENTITY_TYPE2 = 2;
    private static final long ACCOUNT1_ID = 10;
    private static final long ACCOUNT2_ID = 11;
    private static final long REGION1_ID = 20;
    private static final long REGION2_ID = 21;
    private static final long AZ1_ID = 31;
    private static final long AZ2_ID = 32;
    private static final long RT_TOPO_CONTEXT_ID = 777777L;
    private static final double DELTA = 0.001;

    private static final int DEFAULT_CURRENCY = CurrencyAmount.getDefaultInstance().getCurrency();

    private final ComponentCost componentCost = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(3.0).setCurrency(DEFAULT_CURRENCY))
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
            .setCostSourceLink(CostSourceLinkDTO.newBuilder().setCostSource(CostSource.ON_DEMAND_RATE).build())
            .setCostSource(CostSource.ON_DEMAND_RATE)
            .build();
    private final ComponentCost uptimeDiscount = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(.75).setCurrency(DEFAULT_CURRENCY))
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
            .setCostSourceLink(CostSourceLinkDTO.newBuilder()
                    .setCostSource(CostSource.ENTITY_UPTIME_DISCOUNT)
                    .setDiscountCostSourceLink(CostSourceLinkDTO.newBuilder().setCostSource(CostSource.ON_DEMAND_RATE))
                    .build())
            .setCostSource(CostSource.ENTITY_UPTIME_DISCOUNT)
            .build();
    private final ComponentCost componentCost1 = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(2.111).setCurrency(DEFAULT_CURRENCY))
            .setCategory(CostCategory.IP)
            .setCostSourceLink(CostSourceLinkDTO.newBuilder().setCostSource(CostSource.ON_DEMAND_RATE).build())
            .setCostSource(CostSource.ON_DEMAND_RATE)
            .build();
    private final EntityCost entityCost = EntityCost.newBuilder()
            .setAssociatedEntityId(ID1)
            .addComponentCost(componentCost)
            .addComponentCost(uptimeDiscount)
            .addComponentCost(componentCost1)
            .setTotalAmount(CurrencyAmount.newBuilder()
                    .setAmount(1.111)
                    .setCurrency(DEFAULT_CURRENCY)
                    .build())
            .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE1)
            .build();
    private final EntityCost entityCost1 = EntityCost.newBuilder()
            .setAssociatedEntityId(ID2)
            .addComponentCost(componentCost)
            .addComponentCost(uptimeDiscount)
            .addComponentCost(componentCost1)
            .setTotalAmount(CurrencyAmount.newBuilder()
                    .setAmount(1.111)
                    .setCurrency(DEFAULT_CURRENCY)
                    .build())
            .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE2)
            .build();

    private InMemoryEntityCostStore inMemoryStore;
    private DataIngestionBouncer ingestionBouncer;
    private SqlEntityCostStore store;
    private SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());
    private GrpcTestServer testServer = GrpcTestServer.newServer(supplyChainServiceMole);

    /**
     * The clock can't start at too small of a number because TIMESTAMP starts in 1970, but
     * epoch millis starts in 1969.
     */
    private final MutableFixedClock clock = new MutableFixedClock(1_000_000_000);

    @Before
    public void setup() throws Exception {
        testServer.start();
        RepositoryClient repositoryClient = mock(RepositoryClient.class);
        final SupplyChainServiceBlockingStub supplyChainService = SupplyChainServiceGrpc.newBlockingStub(testServer.getChannel());
        when(repositoryClient.getEntitiesByTypePerScope(any(), any())).thenCallRealMethod();
        when(repositoryClient.parseSupplyChainResponseToEntityOidsMap(any())).thenCallRealMethod();

        inMemoryStore = new InMemoryEntityCostStore(repositoryClient, supplyChainService,
                RT_TOPO_CONTEXT_ID);

        ingestionBouncer = mock(DataIngestionBouncer.class);
        when(ingestionBouncer.isTableIngestible(any())).thenReturn(true);

        store = new SqlEntityCostStore(dsl, clock, MoreExecutors.newDirectExecutorService(),
                1, inMemoryStore, ingestionBouncer);
    }

    @Test
    public void testGetCostWithDates() throws DbException, InvalidEntityCostsException {

        // insert
        saveCosts();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter filter = getLastHourFilter();
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(filter);
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        // cached current costs are not deleted.
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    private EntityCostFilter getLastHourFilter() {
        final long startDuration = clock.instant().minus(1, ChronoUnit.HOURS).toEpochMilli();
        final long endDuration = clock.millis();
        return EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                .duration(startDuration, endDuration)
                .build();
    }

    private EntityCostFilter getLatestFilter() {
        return EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                .latestTimestampRequested(true)
                .build();
    }

    @Test
    public void testGetCostWithEntityCostFilterHourEmpty()
            throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = EntityCostFilterBuilder.newBuilder(TimeFrame.HOUR, RT_TOPO_CONTEXT_ID)
                .duration(now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                        now.plusDays(1L).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli())
                .build();

        // insert
        saveCosts();

        // get by date

        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        // cached current costs are not deleted.
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterMonthEmpty()
            throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.MONTH, RT_TOPO_CONTEXT_ID)
                        .duration(now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                                now.plusDays(1L)
                                        .toInstant(OffsetDateTime.now().getOffset())
                                        .toEpochMilli())
                        .build();

        // insert
        saveCosts();

        // get by date

        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        // cached current costs are not deleted.
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterDayEmpty()
            throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = EntityCostFilterBuilder.newBuilder(TimeFrame.DAY, RT_TOPO_CONTEXT_ID)
                .duration(now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                        now.plusDays(1L).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli())
                .build();

        // insert
        saveCosts();

        // get by date
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetCostWithDatesAndEntityIds() throws DbException, InvalidEntityCostsException {

        // insert
        saveCosts();

        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter filter = EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                .entityIds(ImmutableSet.of(1L, 2L))
                .duration(clock.instant().minus(1, ChronoUnit.HOURS).toEpochMilli(), clock.millis())
                .build();
        // get by date with ids.
        final Map<Long, Map<Long, EntityCost>> resultsWithIds = store.getEntityCosts(filter);
        validateResults(resultsWithIds, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        // cached current costs are not deleted.
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterWithEntityId()
            throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter filter = EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                .entityIds(ImmutableSet.of(1L, 2L))
                .duration(clock.instant().minus(1, ChronoUnit.DAYS).toEpochMilli(), clock.millis())
                .build();

        // insert
        saveCosts();

        // get by date

        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(filter);
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterWithEntityTypeFilter()
            throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .entityIds(ImmutableSet.of(1L, 2L))
                        .entityTypes(Collections.singleton(1))
                        .build();

        // insert
        saveCosts();

        // get by date

        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);

        Assert.assertEquals(1, results.size());

        // ensure in the same timestamp, we have expected entity costs
        Assert.assertTrue(
                results.values().stream().allMatch(entityCosts -> entityCosts.size() == 1));

        // ensure we have the right entity costs.
        Assert.assertTrue(results.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.containsKey(ID1)));

        // ensure in the same timestamp, every entity cost have expected component costs
        Assert.assertTrue(results.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.values()
                        .stream()
                        .allMatch(entityCost -> entityCost.getComponentCostCount() == 3)));

        // ensure the components are the same
        Assert.assertTrue(results.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.values()
                        .stream()
                        .allMatch(entityCost ->
                                entityCost.getComponentCostList().contains(componentCost) &&
                                        entityCost.getComponentCostList()
                                                .contains(componentCost1))));

        // clean up
        store.cleanEntityCosts(now);
        // cached entity costs will still be available.
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetCostsWithTwoTimeStamps()
            throws InterruptedException, InvalidEntityCostsException, DbException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(getLastHourFilter());
        validateResults(results, 2, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        // cached entity costs are not cleaned
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetLatestEntityCost()
            throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .latestTimestampRequested(true)
                        .build());
        validateResults(results, 1, 2, 3);

        // cached entity costs are not cleaned
        store.cleanEntityCosts(now);
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetPlanEntityCost()
            throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCosts();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .topologyContextId(2116L)
                        .build());
        validateResults(results, 1, 2, 3);

        // clean up
        store.cleanEntityCosts(now);
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetLatestEntityCostWithOidFilter()
            throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .entityIds(Collections.singleton(1L))
                        .latestTimestampRequested(true)
                        .build());
        Assert.assertEquals(1, results.size());
        // ensure in the same timestamp, we have expected entity costs
        Assert.assertTrue(
                results.values().stream().allMatch(entityCosts -> entityCosts.size() == 1));

        Assert.assertTrue(results.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.values()
                        .stream()
                        .allMatch(entityCost -> entityCost.getComponentCostCount() == 3)));

        final Map<Long, EntityCost> costsBySourceAndCategory = store.getEntityCosts(
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .entityIds(Collections.singleton(1L))
                        .costSources(false,
                                Collections.singleton(CostSource.ON_DEMAND_RATE))
                        .costCategoryFilter(CostCategoryFilter.newBuilder()
                                .setExclusionFilter(false)
                                .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                                .build())
                        .latestTimestampRequested(true)
                        .build()).values().iterator().next();

        Assert.assertEquals(1, costsBySourceAndCategory.get(1L).getComponentCostCount());

        Assert.assertEquals(componentCost, costsBySourceAndCategory.get(1L).getComponentCost(0));
        // clean up should still result in the cached costs for LATEST
        store.cleanEntityCosts(now);
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testGetLatestEntityCostWithTypeFilter()
            throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .entityTypes(Collections.singleton(1))
                        .latestTimestampRequested(true)
                        .build());
        Assert.assertEquals(1, results.size());
        // ensure in the same timestamp, we have expected entity costs
        Assert.assertTrue(
                results.values().stream().allMatch(entityCosts -> entityCosts.size() == 1));
        Assert.assertTrue(results.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.values()
                        .stream()
                        .allMatch(entityCost -> entityCost.getComponentCostCount() == 3)));
        // clean up
        store.cleanEntityCosts(now);
        // cached current costs are not deleted.
        Assert.assertEquals(1, store.getEntityCosts(getLatestFilter()).size());
    }

    @Test
    public void testStoreCostJournal() throws DbException {
        ComponentCost componentCost1 = ComponentCost.newBuilder()
                .setAmount(CurrencyAmount.newBuilder().setAmount(10).setCurrency(DEFAULT_CURRENCY))
                .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                .setCostSourceLink(CostSourceLinkDTO.newBuilder().setCostSource(CostSource.ON_DEMAND_RATE).build())
                .setCostSource(CostSource.ON_DEMAND_RATE)
                .build();
        ComponentCost componentCost11 = ComponentCost.newBuilder()
                .setAmount(CurrencyAmount.newBuilder().setAmount(-3).setCurrency(DEFAULT_CURRENCY))
                .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                .setCostSourceLink(CostSourceLinkDTO.newBuilder().setCostSource(CostSource.BUY_RI_DISCOUNT).build())
                .setCostSource(CostSource.BUY_RI_DISCOUNT)
                .build();

        ComponentCost componentCost2 = ComponentCost.newBuilder()
                .setAmount(CurrencyAmount.newBuilder().setAmount(10).setCurrency(DEFAULT_CURRENCY))
                .setCategory(CostCategory.ON_DEMAND_LICENSE)
                .setCostSourceLink(CostSourceLinkDTO.newBuilder().setCostSource(CostSource.ON_DEMAND_RATE).build())
                .setCostSource(CostSource.ON_DEMAND_RATE)
                .build();
        ComponentCost componentCost22 = ComponentCost.newBuilder()
                .setAmount(CurrencyAmount.newBuilder().setAmount(-5).setCurrency(DEFAULT_CURRENCY))
                .setCategory(CostCategory.ON_DEMAND_LICENSE)
                .setCostSourceLink(CostSourceLinkDTO.newBuilder().setCostSource(CostSource.BUY_RI_DISCOUNT).build())
                .setCostSource(CostSource.BUY_RI_DISCOUNT)
                .build();

        EntityCost entityCost1 = EntityCost.newBuilder()
                .setAssociatedEntityId(ID1)
                .addComponentCost(componentCost1)
                .addComponentCost(componentCost11)
                .addComponentCost(componentCost2)
                .addComponentCost(componentCost22)
                .setTotalAmount(CurrencyAmount.newBuilder()
                        .setAmount(1.111)
                        .setCurrency(DEFAULT_CURRENCY)
                        .build())
                .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE1)
                .build();
        ImmutableMap<CostSource, Double> computeSourceCostMap = ImmutableMap.of(
                CostSource.ON_DEMAND_RATE, 10.0,
                CostSource.BUY_RI_DISCOUNT, -3.0);
        ImmutableMap<CostSource, Double> licenseSourceCostMap = ImmutableMap.of(
                CostSource.ON_DEMAND_RATE, 10.0,
                CostSource.BUY_RI_DISCOUNT, -5.0);
        ImmutableMap<CostCategory, Map<CostSource, Double>> categorySourceCostMap = ImmutableMap.of(
                CostCategory.ON_DEMAND_COMPUTE, computeSourceCostMap,
                CostCategory.ON_DEMAND_LICENSE, licenseSourceCostMap);
        final CostJournal<TopologyEntityDTO> journal1 =
                mockCostJournalWithCostSources(ID1, ASSOCIATED_ENTITY_TYPE1,
                        categorySourceCostMap);
        final CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);
        Mockito.when(topology.getOwner(org.mockito.Matchers.anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(topology.getConnectedAvailabilityZone(org.mockito.Matchers.anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(topology.getConnectedRegion(org.mockito.Matchers.anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(journal1.toEntityCostProto()).thenReturn(entityCost1);

        store.persistEntityCost(ImmutableMap.of(ID1, journal1), topology, clock.millis(), false);

        final Map<Long, Map<Long, EntityCost>> costs = store.getEntityCosts(
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .duration(clock.millis(), clock.millis())
                        .build());
        Assert.assertNotNull(costs);
        final Map<Long, EntityCost> costMap = costs.values().iterator().next();
        Assert.assertNotNull(costMap);
        final EntityCost entityCost = costMap.get(ID1);
        Assert.assertNotNull(entityCost);
        MatcherAssert.assertThat(entityCost.getAssociatedEntityType(),
                CoreMatchers.is(ASSOCIATED_ENTITY_TYPE1));
        MatcherAssert.assertThat(entityCost.getAssociatedEntityId(), CoreMatchers.is(ID1));
        MatcherAssert.assertThat(entityCost.getComponentCostCount(), CoreMatchers.is(4));
        MatcherAssert.assertThat(entityCost.getComponentCostList(), Matchers.containsInAnyOrder(
                ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .setCostSource(CostSource.ON_DEMAND_RATE)
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                                .setAmount(10.0))
                        .build(),
                ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .setCostSource(CostSource.BUY_RI_DISCOUNT)
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                                .setAmount(-3.0))
                        .build(),
                ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_LICENSE)
                        .setCostSource(CostSource.ON_DEMAND_RATE)
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                                .setAmount(10.0))
                        .build(),
                ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_LICENSE)
                        .setCostSource(CostSource.BUY_RI_DISCOUNT)
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                                .setAmount(-5.0))
                        .build()

        ));
    }

    /**
     * Test the case when we retrieve entity costs based on the account id.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetCostsForAccount() throws Exception {
        // ARRANGE
        final EntityCostFilter entityCostFilter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .accountIds(Collections.singleton(ACCOUNT1_ID))
                        .build();

        // insert
        saveCosts();

        // ACT
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);

        //ASSERT
        MatcherAssert.assertThat(results.size(), CoreMatchers.is(1));
        final Map<Long, EntityCost> entityMap = results.values().iterator().next();
        MatcherAssert.assertThat(entityMap.size(), CoreMatchers.is(1));
        MatcherAssert.assertThat(entityMap.keySet().iterator().next(), CoreMatchers.is(ID1));
        MatcherAssert.assertThat(entityMap.get(ID1),
                CoreMatchers.is(entityCost.toBuilder().build()));

        store.cleanEntityCosts(LocalDateTime.now(clock));
    }

    /**
     * Test the case when we retrieve entity costs based on the availability zone id.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetCostsForAvailabilityZone() throws Exception {
        // ARRANGE
        final EntityCostFilter entityCostFilter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .availabilityZoneIds(Collections.singleton(AZ1_ID))
                        .build();

        // insert
        saveCosts();

        // ACT
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);

        //ASSERT
        MatcherAssert.assertThat(results.size(), CoreMatchers.is(1));
        final Map<Long, EntityCost> entityMap = results.values().iterator().next();
        MatcherAssert.assertThat(entityMap.size(), CoreMatchers.is(1));
        MatcherAssert.assertThat(entityMap.keySet().iterator().next(), CoreMatchers.is(ID1));
        MatcherAssert.assertThat(entityMap.get(ID1),
                CoreMatchers.is(entityCost.toBuilder().build()));

        store.cleanEntityCosts(LocalDateTime.now(clock));
    }

    /**
     * Test the case when we retrieve entity costs based on the region id.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetCostsForRegion() throws Exception {
        // ARRANGE
        final EntityCostFilter entityCostFilter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .regionIds(Collections.singleton(REGION1_ID))
                        .build();

        // insert
        saveCosts();

        // ACT
        final Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);

        //ASSERT
        MatcherAssert.assertThat(results.size(), CoreMatchers.is(1));
        final Map<Long, EntityCost> entityMap = results.values().iterator().next();
        MatcherAssert.assertThat(entityMap.size(), CoreMatchers.is(1));
        MatcherAssert.assertThat(entityMap.keySet().iterator().next(), CoreMatchers.is(ID1));
        MatcherAssert.assertThat(entityMap.get(ID1),
                CoreMatchers.is(entityCost.toBuilder().build()));

        store.cleanEntityCosts(LocalDateTime.now(clock));
    }

    /**
     * Test getting entity cost stat records for specific time frame.
     *
     * @throws Exception when failed
     */
    @Test
    public void testGetEntityCostStatRecordsForTimeframe() throws Exception {
        // ARRANGE
        inMemoryStore.updateEntityCosts(Collections.singletonList(EntityCost.newBuilder()
                .setAssociatedEntityId(ID1)
                .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addComponentCost(ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .setCostSourceLink(CostSourceLinkDTO.newBuilder()
                                .setCostSource(CostSource.ON_DEMAND_RATE)
                                .build())
                        .setAmount(CurrencyAmount.newBuilder().setAmount(100).setCurrency(1))
                        .build())
                .build()));

        dsl.insertInto(Tables.ENTITY_COST).values(1L, LocalDateTime.of(2000, 1, 1, 1, 1, 1), 1, 1,
                1, BigDecimal.valueOf(100), 1, 1L, 1L, 1L).execute();

        final Object[] values = {1L, LocalDateTime.of(2000, 1, 1, 1, 1, 1), 1, 1, 1,
                BigDecimal.valueOf(100), 1, 1L, 1L, 1L};
        Stream.of(Tables.ENTITY_COST_BY_HOUR, Tables.ENTITY_COST_BY_DAY,
                Tables.ENTITY_COST_BY_MONTH).forEach(table -> dsl.insertInto(table)
                .values(values)
                .execute());

        // ASSERT
        final TriConsumer<String, Double, Map<Long, Collection<StatRecord>>> checkResult =
                (String expectedUnits, Double expectedAmount, Map<Long, Collection<StatRecord>> entityCostStatRecords) -> {
                    Assert.assertEquals(1, entityCostStatRecords.size());
                    final Collection<StatRecord> statRecords =
                            entityCostStatRecords.values().iterator().next();
                    Assert.assertEquals(1, statRecords.size());
                    final StatRecord statRecord = statRecords.iterator().next();
                    Assert.assertEquals(expectedUnits, statRecord.getUnits());
                    final StatValue v = statRecord.getValues();
                    Assert.assertEquals(expectedAmount, v.getMax(), DELTA);
                    Assert.assertEquals(expectedAmount, v.getMin(), DELTA);
                    Assert.assertEquals(expectedAmount, v.getTotal(), DELTA);
                    Assert.assertEquals(expectedAmount, v.getAvg(), DELTA);
                };

        final Object[][] testCases = {
                {TimeFrame.LATEST, 100D, true},
                {TimeFrame.HOUR, 100D, true},
                {TimeFrame.DAY, 2400D, true},
                {TimeFrame.MONTH, 73000D, true},
                {TimeFrame.YEAR, 876000D, true},
                {TimeFrame.LATEST, 100D, false},
                {TimeFrame.HOUR, 100D, false},
                {TimeFrame.DAY, 100D, false},
                {TimeFrame.MONTH, 100D, false},
                {TimeFrame.YEAR, 100D, false}
        };

        for (final Object[] data : testCases) {
            final TimeFrame timeFrame = (TimeFrame)data[0];
            final double amount = (double)data[1];
            final boolean totalValuesRequested = (boolean)data[2];

            final String units =
                    totalValuesRequested ? timeFrame.getUnits() : TimeFrame.HOUR.getUnits();

            // ACT
            checkResult.accept(units, amount,
                    store.getEntityCostStats(EntityCostFilterBuilder.newBuilder(timeFrame,
                            RT_TOPO_CONTEXT_ID)
                            .entityIds(Collections.singleton(ID1))
                            .requestedGroupByEnums(Collections.singletonList(GroupBy.COST_CATEGORY))
                            .totalValuesRequested(totalValuesRequested)
                            .build()));

            // ACT. Group by branch.
            checkResult.accept(units, amount,
                    store.getEntityCostStats(EntityCostFilterBuilder.newBuilder(timeFrame,
                            RT_TOPO_CONTEXT_ID)
                            .entityIds(Collections.singleton(ID1))
                            .groupByFields(Collections.singleton(
                                    ENTITY_COST.ASSOCIATED_ENTITY_TYPE.getName()))
                            .requestedGroupByEnums(Collections.singletonList(GroupBy.ENTITY_TYPE))
                            .totalValuesRequested(totalValuesRequested)
                            .build()));
        }

        // CLEANUP
        Stream.of(Tables.ENTITY_COST, Tables.ENTITY_COST_BY_HOUR, Tables.ENTITY_COST_BY_DAY,
                Tables.ENTITY_COST_BY_MONTH).forEach(table -> dsl.deleteFrom(table).execute());
    }

    /**
     * Test the case when we retrieve entity cost stats with a group by.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetLatestCostStatsWithGroupBy() throws Exception {
        // ARRANGE
        final EntityCostFilter entityCostFilter =
                EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, RT_TOPO_CONTEXT_ID)
                        .entityIds(Collections.singleton(ID1))
                        .requestedGroupByEnums(Collections.singletonList(GroupBy.COST_CATEGORY))
                        .build();

        // insert
        saveCosts();

        // ACT
        final Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);

        //ASSERT
        MatcherAssert.assertThat(results.size(), CoreMatchers.is(1));
        final Collection<StatRecord> statRecords = results.values().iterator().next();
        MatcherAssert.assertThat(statRecords.size(), CoreMatchers.is(3));
        entityCost.getComponentCostList().stream()
                .forEach(componentCost -> statRecords.stream()
                    .filter(statRecord -> statRecord.getCategory() == componentCost.getCategory()
                            && statRecord.getCostSource() == componentCost.getCostSource())
                    .forEach(statRecord ->
                            Assert.assertEquals(statRecord.getValues().getTotal(),
                                    componentCost.getAmount().getAmount(), 0.001)));

        store.cleanEntityCosts(LocalDateTime.now(clock));
    }

    private CostJournal<TopologyEntityDTO> mockCostJournalWithCostSources(
            final long entityId, final int entityType,
            final Map<CostCategory, Map<CostSource, Double>> costsByCategoryAndSource) {

        final TopologyEntityDTO entity =
                TopologyEntityDTO.newBuilder().setOid(entityId).setEntityType(entityType).build();
        final CostJournal<TopologyEntityDTO> journal = mock(CostJournal.class);
        Mockito.when(journal.getEntity()).thenReturn(entity);
        Mockito.when(journal.getCategories()).thenReturn(costsByCategoryAndSource.keySet());

        for (final Map.Entry<CostCategory, Map<CostSource, Double>> entry : costsByCategoryAndSource.entrySet()) {
            CostCategory category = entry.getKey();
            Map<CostSource, Double> costsBySource = entry.getValue();
            Map<CostSource, TraxNumber> costTraxBySource = costsBySource.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> trax(e.getValue())));

            Mockito.when(journal.getFilteredCategoryCostsBySource(category, CostJournal.CostSourceFilter.EXCLUDE_UPTIME))
                    .thenReturn(costTraxBySource.entrySet().stream()
                            .filter(e -> e.getKey() != CostSource.ENTITY_UPTIME_DISCOUNT)
                            .collect(ImmutableMap.toImmutableMap(
                                    Map.Entry::getKey,
                                    Map.Entry::getValue)));
            Mockito.when(journal.getFilteredCategoryCostsBySource(category, CostSourceFilter.INCLUDE_ALL))
                    .thenReturn(costTraxBySource);

        }
        return journal;
    }


    private CostJournal<TopologyEntityDTO> mockCostJournal(final EntityCost entityCost) {
        Map<CostCategory, Map<CostSource, Double>> costCategorySourceMap = entityCost.getComponentCostList()
                .stream()
                .collect(Collectors.groupingBy(
                        ComponentCost::getCategory,
                        Collectors.groupingBy(
                                c -> c.getCostSourceLink().getCostSource(),
                                Collectors.summingDouble(c -> c.getAmount().getAmount())
                        )));
        CostJournal journal = mockCostJournalWithCostSources(entityCost.getAssociatedEntityId(),
                entityCost.getAssociatedEntityType(),
                costCategorySourceMap);
        Mockito.when(journal.toEntityCostProto()).thenReturn(entityCost);
        return journal;
    }

    private void validateResults(final Map<Long, Map<Long, EntityCost>> map,
            final int expectedSizeOfEntries, final int expectedSizeOfEntityCosts,
            final int expectedSizeOfComponentCosts) {
        // ensure have expected entries (timestamps)
        Assert.assertEquals(expectedSizeOfEntries, map.size());

        // ensure in the same timestamp, we have expected entity costs
        Assert.assertTrue(map.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.size() == expectedSizeOfEntityCosts));

        // ensure we have the right entity costs.
        Assert.assertTrue(map.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.containsKey(ID1) && entityCosts.containsKey(ID2)));

        // ensure in the same timestamp, every entity cost have expected component costs
        Assert.assertTrue(map.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.values()
                        .stream()
                        .allMatch(entityCost -> entityCost.getComponentCostCount() ==
                                expectedSizeOfComponentCosts)));

        // ensure the components are the same
        Assert.assertTrue(map.values()
                .stream()
                .allMatch(entityCosts -> entityCosts.values()
                        .stream()
                        .allMatch(cost -> validateComponentCost(cost,
                                ImmutableList.of(componentCost, componentCost1)))));
    }

    private boolean validateComponentCost(final EntityCost entityCost,
                                          final List<ComponentCost> componentCosts) {
        // Set source link since DB
        List<ComponentCost> costs = entityCost.getComponentCostList().stream().map(c ->
                c.toBuilder().setCostSourceLink(CostSourceLinkDTO.newBuilder()
                        .setCostSource(c.getCostSource())).build()).collect(Collectors.toList());
        return costs.contains(componentCost) && costs.contains(componentCost1);
    }

    private void saveCosts() throws DbException, InvalidEntityCostsException {
        final TopologyEntityDTO account1EntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(ACCOUNT1_ID)
                .build();
        final TopologyEntityDTO region1EntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION1_ID)
                .build();
        final TopologyEntityDTO az1EntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setOid(AZ1_ID)
                .build();
        final TopologyEntityDTO account2EntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(ACCOUNT2_ID)
                .build();
        final TopologyEntityDTO region2EntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION2_ID)
                .build();
        final TopologyEntityDTO az2EntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setOid(AZ2_ID)
                .build();

        final CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);
        Mockito.when(topology.getOwner(ID1)).thenReturn(Optional.of(account1EntityDTO));
        Mockito.when(topology.getConnectedAvailabilityZone(ID1))
                .thenReturn(Optional.of(az1EntityDTO));
        Mockito.when(topology.getConnectedRegion(ID1)).thenReturn(Optional.of(region1EntityDTO));
        Mockito.when(topology.getOwner(ID2)).thenReturn(Optional.of(account2EntityDTO));
        Mockito.when(topology.getConnectedAvailabilityZone(ID2))
                .thenReturn(Optional.of(az2EntityDTO));
        Mockito.when(topology.getConnectedRegion(ID2)).thenReturn(Optional.of(region2EntityDTO));
        final HashMap<Long, CostJournal<TopologyEntityDTO>> costJournal = new HashMap<>();
        costJournal.put(entityCost.getAssociatedEntityId(), mockCostJournal(entityCost));
        costJournal.put(entityCost1.getAssociatedEntityId(), mockCostJournal(entityCost1));
        store.persistEntityCost(costJournal, topology, clock.millis(), false);
        store.persistEntityCost(costJournal, topology, 2116L, true);
    }

    private void saveCostsWithTwoTimeStamps() throws DbException {
        final CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);
        Mockito.when(topology.getOwner(org.mockito.Matchers.anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(topology.getConnectedAvailabilityZone(org.mockito.Matchers.anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(topology.getConnectedRegion(org.mockito.Matchers.anyLong()))
                .thenReturn(Optional.empty());
        final HashMap<Long, CostJournal<TopologyEntityDTO>> costJournal = new HashMap<>();
        costJournal.put(entityCost.getAssociatedEntityId(), mockCostJournal(entityCost));
        costJournal.put(entityCost1.getAssociatedEntityId(), mockCostJournal(entityCost1));
        store.persistEntityCost(costJournal, topology, clock.millis(), false);
        clock.changeInstant(clock.instant().plusMillis(1000));
        store.persistEntityCost(costJournal, topology, clock.millis(), false);
    }
}
