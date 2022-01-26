package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Verify operation of the entity savings tracker.
 */
@RunWith(Parameterized.class)
public class EntitySavingsTrackerTest extends MultiDbTestBase {

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
    public EntitySavingsTrackerTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost", TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private static EntitySavingsStore<DSLContext> entitySavingsStore;

    private EntityStateStore<DSLContext> entityStateStore;

    private static EntityEventsJournal entityEventsJournal;

    private EntitySavingsTracker tracker;

    private static final Calendar calendar = Calendar.getInstance();

    private static LocalDateTime time0900am = LocalDateTime.of(2021, 3, 23, 9, 0);
    private static LocalDateTime time0915am = LocalDateTime.of(2021, 3, 23, 9, 15);
    private static LocalDateTime time0945am = LocalDateTime.of(2021, 3, 23, 9, 45);
    private static LocalDateTime time1000am = LocalDateTime.of(2021, 3, 23, 10, 0);
    private static LocalDateTime time1100am = LocalDateTime.of(2021, 3, 23, 11, 0);
    private static LocalDateTime time1130am = LocalDateTime.of(2021, 3, 23, 11, 30);
    private static LocalDateTime time1200pm = LocalDateTime.of(2021, 3, 23, 12, 0);

    private static final long vm1Id = 101L;
    private static final long vm2Id = 201L;
    private static final long vm3Id = 301L;
    private static final long vm4Id = 401L;

    private static final long action1Id = 1001L;
    private static final long action2Id = 1002L;
    private static final long action3Id = 1003L;

    private static final long region1Id = 2000L;
    private static final long region2Id = 2001L;

    private static final long availabilityZone1Id = 3001;

    private static final long account1Id = 4000L;
    private static final long account2Id = 4001L;

    private static final long serviceProvider1Id = 5000L;
    private static final long serviceProvider2Id = 5001L;

    private static RepositoryClient repositoryClient;

    // Maps the period start time to a list of events in the period that start at the start time and ends 1 hour later.
    private static final Map<LocalDateTime, List<SavingsEvent>> eventsByPeriod = new HashMap<>();

    private static Clock clock = Clock.systemUTC();

    private long realtimeTopologyContextId = 777777;

    private static final long ACTION_EXPIRATION_TIME = TimeUnit.HOURS.toMillis(1L);

    private final Set<Long> uuids = Collections.emptySet();

    @Captor
    private ArgumentCaptor<Set<EntitySavingsStats>> statsCaptor;

    @Captor
    private ArgumentCaptor<List<Long>> entityOidListCaptor;

    /**
     * Set up before each test case.
     *
     * @throws Exception any exception
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        entityEventsJournal = mock(EntityEventsJournal.class);
        createEvents();
        final long time0900amMillis = TimeUtil.localDateTimeToMilli(time0900am, clock);
        final long time1000amMillis = TimeUtil.localDateTimeToMilli(time1000am, clock);
        final long time1100amMillis = TimeUtil.localDateTimeToMilli(time1100am, clock);
        final long time1200pmMillis = TimeUtil.localDateTimeToMilli(time1200pm, clock);
        when(entityEventsJournal.removeEventsBetween(time0900amMillis, time1000amMillis, uuids))
                .thenReturn(eventsByPeriod.get(time0900am));
        when(entityEventsJournal.removeEventsBetween(time1000amMillis, time1100amMillis, uuids))
                .thenReturn(eventsByPeriod.get(time1000am));
        when(entityEventsJournal.removeEventsBetween(time1100amMillis, time1200pmMillis, uuids))
                .thenReturn(eventsByPeriod.get(time1100am));
        entitySavingsStore = new SqlEntitySavingsStore(dsl, clock, 5);
        entityStateStore = mock(SqlEntityStateStore.class);

        setupRepositoryClient();
        tracker = spy(new EntitySavingsTracker(entitySavingsStore, entityEventsJournal,
                entityStateStore, Clock.systemUTC(), mock(TopologyEntityCloudTopologyFactory.class),
                repositoryClient, dsl, realtimeTopologyContextId, 2));

        Set<EntityState> stateSet = ImmutableSet.of(
                createEntityState(vm1Id, 2d, null, null, null),
                createEntityState(vm2Id, null, null, null, 0d),
                createEntityState(vm3Id, 1d, 2d, 3d, 4d),
                createEntityState(vm4Id, 1d, null, null, null));
        Answer<Stream> stateStream = new Answer<Stream>() {
            @Override
            public Stream answer(InvocationOnMock invocation) throws Throwable {
                return stateSet.stream();
            }
        };
        when(entityStateStore.getAllEntityStates(any(DSLContext.class))).thenAnswer(stateStream);
    }

    private void setupRepositoryClient() {
        repositoryClient = mock(RepositoryClient.class);

        // Create topology entities for vm1 and vm2. These two VMs have events (see eventsByPeriod)
        List<Long> vmIds = Arrays.asList(vm1Id, vm2Id);
        TopologyEntityDTO vm1EntityDto = TopologyEntityDTO.newBuilder()
                .setOid(vm1Id)
                .setEntityType(10)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(region1Id)
                        .setConnectedEntityType(EntityType.REGION_VALUE)
                        .build())
                .build();
        TopologyEntityDTO vm2EntityDto = TopologyEntityDTO.newBuilder()
                .setOid(vm2Id)
                .setEntityType(10)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(availabilityZone1Id)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .build())
                .build();

        TopologyEntityDTO region1 = TopologyEntityDTO.newBuilder()
                .setOid(region1Id)
                .setEntityType(EntityType.REGION_VALUE)
                .build();

        TopologyEntityDTO region2 = TopologyEntityDTO.newBuilder()
                .setOid(region2Id)
                .setEntityType(EntityType.REGION_VALUE)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .setConnectedEntityId(availabilityZone1Id)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .build())
                .build();

        TopologyEntityDTO serviceProvider1 = TopologyEntityDTO.newBuilder()
                .setOid(serviceProvider1Id)
                .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
                .build();
        TopologyEntityDTO serviceProvider2 = TopologyEntityDTO.newBuilder()
                .setOid(serviceProvider2Id)
                .setEntityType(EntityType.SERVICE_PROVIDER_VALUE)
                .build();

        when(repositoryClient.retrieveTopologyEntities(vmIds, realtimeTopologyContextId))
                .thenReturn(Stream.of(vm1EntityDto, vm2EntityDto));

        when(repositoryClient.retrieveTopologyEntities(Collections.EMPTY_LIST, realtimeTopologyContextId))
                .thenReturn(Stream.empty());

        when(repositoryClient.retrieveTopologyEntities(Collections.singletonList(vm1Id), realtimeTopologyContextId))
                .thenReturn(Stream.of(vm1EntityDto, vm2EntityDto));

        when(repositoryClient.getAllBusinessAccountOidsInScope(ImmutableSet.of(vm1Id, vm2Id)))
                .thenReturn(ImmutableSet.of(account1Id, account2Id));

        Answer<Stream> regionServiceProviderStream = new Answer<Stream>() {
            @Override
            public Stream answer(InvocationOnMock invocation) throws Throwable {
                return Stream.of(region1, region2, serviceProvider1, serviceProvider2);
            }
        };
        when(repositoryClient.getEntitiesByType(Arrays.asList(EntityType.REGION, EntityType.SERVICE_PROVIDER)))
                .thenAnswer(regionServiceProviderStream);
    }

    private static void createEvents() {
        final long time0915amMillis = TimeUtil.localDateTimeToMilli(time0915am, clock);
        final long time0945amMillis = TimeUtil.localDateTimeToMilli(time0945am, clock);
        final long time1130amMillis = TimeUtil.localDateTimeToMilli(time1130am, clock);
        eventsByPeriod.put(time0900am, Arrays.asList(
                getActionEvent(vm1Id, time0915amMillis, ActionEventType.RECOMMENDATION_ADDED, action1Id),
                getActionEvent(vm1Id, time0915amMillis, ActionEventType.SCALE_EXECUTION_SUCCESS, action1Id),
                getActionEvent(vm2Id, time0945amMillis, ActionEventType.RECOMMENDATION_ADDED, action2Id),
                getActionEvent(vm2Id, time0945amMillis, ActionEventType.SCALE_EXECUTION_SUCCESS, action2Id)));
        eventsByPeriod.put(time1000am, new ArrayList<>());
        eventsByPeriod.put(time1100am, Arrays.asList(
                getActionEvent(vm1Id, time1130amMillis, ActionEventType.RECOMMENDATION_ADDED, action3Id),
                getActionEvent(vm1Id, time1130amMillis, ActionEventType.SCALE_EXECUTION_SUCCESS, action3Id)));
    }

    @Nonnull
    private static SavingsEvent getActionEvent(long vmId, long timestamp, ActionEventType actionType,
                                               long actionId) {
        final EntityPriceChange priceChange = new EntityPriceChange.Builder()
                .sourceOid(1001L)
                .sourceCost(10.518d)
                .destinationOid(2001L)
                .destinationCost(6.23d)
                .build();
        return new SavingsEvent.Builder()
                .actionEvent(new ActionEvent.Builder()
                        .actionId(actionId)
                        .eventType(actionType).build())
                .entityId(vmId)
                .timestamp(timestamp)
                .entityPriceChange(priceChange)
                .build();
    }

    /**
     * Verify the DSLContext passed into methods of entityStateStore and entitySavingsStore are not
     * the same as the one passed into the constructor of EntitySavingsTracker. The transactional
     * DSLContext should be used.
     *
     * @throws Exception any exception
     */
    @Test
    public void verifyCorrectDslContextUsed() throws Exception {
        tracker.processEvents(time0900am, time1000am, uuids);
        final long startTimeMillis = TimeUtil.localDateTimeToMilli(time0900am, clock);
        ArgumentCaptor<DSLContext> dslCaptor = ArgumentCaptor.forClass(DSLContext.class);
        verify(tracker).generateStats(eq(startTimeMillis), dslCaptor.capture(), any());
        DSLContext generateStatesDsl = dslCaptor.getValue();
        assertNotEquals(dsl, generateStatesDsl);

        ArgumentCaptor<DSLContext> clearUpdatedFlagsDslCaptor = ArgumentCaptor.forClass(DSLContext.class);
        verify(entityStateStore).clearUpdatedFlags(clearUpdatedFlagsDslCaptor.capture(), eq(uuids));
        DSLContext clearUpdateFlagsDsl = clearUpdatedFlagsDslCaptor.getValue();
        assertNotEquals(dsl, clearUpdateFlagsDsl);

        ArgumentCaptor<DSLContext> updateStateDslCaptor = ArgumentCaptor.forClass(DSLContext.class);
        verify(entityStateStore).updateEntityStates(anyMap(), any(TopologyEntityCloudTopology.class),
                updateStateDslCaptor.capture(), eq(uuids));
        DSLContext updateStateDsl = updateStateDslCaptor.getValue();
        assertNotEquals(dsl, updateStateDsl);

        ArgumentCaptor<DSLContext> deleteEntityStatesDslCaptor = ArgumentCaptor.forClass(DSLContext.class);
        verify(entityStateStore).deleteEntityStates(anySet(), deleteEntityStatesDslCaptor.capture());
        DSLContext deleteEntityStatesDsl = deleteEntityStatesDslCaptor.getValue();
        assertNotEquals(dsl, deleteEntityStatesDsl);
    }

    /**
     * Call the process method with start and end time at 9am and 10am respectively.
     * Process the period 9:00 - 10:00.
     *
     * @throws Exception exceptions
     */
    @Test
    public void processWithOnePeriod() throws Exception {
        tracker.processEvents(time0900am, time1000am, uuids);
        final long startTimeMillis = TimeUtil.localDateTimeToMilli(time0900am, clock);
        final long endTimeMillis = TimeUtil.localDateTimeToMilli(time1000am, clock);
        verify(entityEventsJournal).removeEventsBetween(startTimeMillis, endTimeMillis, uuids);
        verify(tracker).generateStats(eq(startTimeMillis), any(DSLContext.class), eq(uuids));
        List<Long> vmIds = Arrays.asList(vm1Id, vm2Id);
        verify(repositoryClient).getAllBusinessAccountOidsInScope(new HashSet<>(vmIds));
        verify(repositoryClient).getEntitiesByType(Arrays.asList(EntityType.REGION, EntityType.SERVICE_PROVIDER));

        verify(repositoryClient, times(2)).retrieveTopologyEntities(entityOidListCaptor.capture(), eq(realtimeTopologyContextId));
        List<List<Long>> capturedEntityLists = entityOidListCaptor.getAllValues();
        Assert.assertEquals(2, capturedEntityLists.get(0).size());
        Assert.assertTrue(capturedEntityLists.get(0).containsAll(vmIds));
        Assert.assertEquals(9, capturedEntityLists.get(1).size());
        Assert.assertTrue(capturedEntityLists.get(1).containsAll(Arrays.asList(vm1Id, vm2Id,
                region1Id, region2Id, availabilityZone1Id, account1Id, account2Id, serviceProvider1Id,
                serviceProvider2Id)));

        // Assert that data is inserted into entity_savings_by_hour table.
        List<AggregatedSavingsStats> statsReadBack = getSavingsStats();
        assertTrue(statsReadBack.size() > 0);
    }

    /**
     * Call the process method with start and end time at 9am and 11am respectively.
     * Evbe processed in two rounds: 9:00 - 10:00 and 10:00 - 11:00.
     *
     * @throws Exception exceptions
     */
    @Test
    public void processWithTwoPeriods() throws Exception {
        final long time0900amMillis = TimeUtil.localDateTimeToMilli(time0900am, clock);
        final long time1000amMillis = TimeUtil.localDateTimeToMilli(time1000am, clock);
        final long time1100amMillis = TimeUtil.localDateTimeToMilli(time1100am, clock);
        tracker.processEvents(time0900am, time1100am, uuids);
        verify(entityEventsJournal).removeEventsBetween(0, time0900amMillis, uuids);
        verify(entityEventsJournal).removeEventsBetween(time0900amMillis, time1000amMillis, uuids);
        verify(tracker).generateStats(eq(time0900amMillis), any(DSLContext.class), eq(uuids));
        verify(entityEventsJournal).removeEventsBetween(time1000amMillis, time1100amMillis, uuids);
        verify(tracker).generateStats(eq(time1000amMillis), any(DSLContext.class), eq(uuids));
        verify(tracker, times(2)).generateStats(anyLong(), any(DSLContext.class), eq(uuids));

        // Assert that data is inserted into entity_savings_by_hour table.
        List<AggregatedSavingsStats> statsReadBack = getSavingsStats();
        assertTrue(statsReadBack.size() > 0);
    }

    /**
     * If any database exception is thrown when updating entity_savings_by_hour or entity_savings_state
     * tables, all database updates will be rolled back.
     *
     * @throws Exception any exceptions
     */
    @Test
    public void testDatabaseRollback() throws Exception {
        doThrow(EntitySavingsException.class).when(entityStateStore)
                .deleteEntityStates(anySetOf(Long.class), any(DSLContext.class));
        List<Long> hourlyStatsTimes = tracker.processEvents(time0900am, time1000am, uuids);
        List<AggregatedSavingsStats> statsReadBack = getSavingsStats();

        // There should be no data in the entity_savings_by_hour table because a rollback occurred.
        assertEquals(0, statsReadBack.size());
        assertEquals(0, hourlyStatsTimes.size());

        // Events should be added back to the journal after the failure.
        verify(entityEventsJournal).addEvents(any(List.class));
    }

    private List<AggregatedSavingsStats> getSavingsStats() throws EntitySavingsException {
        final Set<EntitySavingsStatsType> allStatsTypes = Arrays.stream(EntitySavingsStatsType
                .values()).collect(Collectors.toSet());
        Collection<Integer> entityTypes = Collections.singleton(EntityType.VIRTUAL_MACHINE_VALUE);
        List<AggregatedSavingsStats> statsReadBack = entitySavingsStore.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(time0900am, clock),
                TimeUtil.localDateTimeToMilli(time1000am, clock),
                ImmutableSet.of(vm1Id, vm2Id, vm3Id, vm4Id), entityTypes, new HashSet<>());
        return statsReadBack;
    }

    /**
     * Test the generateStats method.
     *
     * @throws Exception exceptions
     */
    @Test
    public void testGenerateStats() throws Exception {
        final long time1000amMillis = TimeUtil.localDateTimeToMilli(time1000am, clock);
        EntitySavingsStore<DSLContext> entitySavingsStore = mock(EntitySavingsStore.class);
        EntitySavingsTracker tracker = spy(new EntitySavingsTracker(entitySavingsStore, entityEventsJournal,
                entityStateStore, Clock.systemUTC(), mock(TopologyEntityCloudTopologyFactory.class),
                repositoryClient, dsl, realtimeTopologyContextId, 2));
        tracker.generateStats(time1000amMillis, dsl, uuids);

        // Make sure we call the version of getAllEntityStates that accepts DSL context as parameter.
        verify(entityStateStore).getAllEntityStates(dsl);

        // addHourlyStats is called three times.
        // First 2 states will generate 2 stats records => 1 call
        // Third state will generate 4 stats records => 1 call
        // Forth state will generate 1 stats (less than 1 page) and flushed at the end => 1 call
        verify(entitySavingsStore, times(3)).addHourlyStats(statsCaptor.capture(), any(DSLContext.class));
    }

    /**
     * Verify that stats are only generated for the provided list of UUIDs.  The test state set
     * contains vm1Id, vm2Id, vm3Id, and vm4Id.  The test will scope to vm2Id and vm4Id, so stats
     * for only those entities will be generated.
     *
     * @throws Exception exceptions
     */
    @Test
    public void testGenerateStatsScopedUuids() throws Exception {
        final Set<Long> scopedUuids = ImmutableSet.of(vm2Id, vm4Id);
        final long time1000amMillis = TimeUtil.localDateTimeToMilli(time1000am, clock);
        EntitySavingsStore<DSLContext> entitySavingsStore = mock(EntitySavingsStore.class);
        EntitySavingsTracker tracker = spy(new EntitySavingsTracker(entitySavingsStore, entityEventsJournal,
                entityStateStore, Clock.systemUTC(), mock(TopologyEntityCloudTopologyFactory.class),
                repositoryClient, dsl, realtimeTopologyContextId, 1000));
        tracker.generateStats(time1000amMillis, dsl, scopedUuids);

        // Make sure we call the version of getAllEntityStates that accepts DSL context as parameter.
        verify(entityStateStore).getAllEntityStates(dsl);
        verify(entitySavingsStore).addHourlyStats(statsCaptor.capture(), any(DSLContext.class));
        // Verify that EntitySavingsTracker.stateToStats is called only for UUIDs 201 and 401.
        assertEquals(1, statsCaptor.getAllValues().size());
        Set<EntitySavingsStats> stats = statsCaptor.getValue();
        assertEquals(2, stats.size());
        assertTrue(ImmutableSet.of(vm2Id, vm4Id).containsAll(stats.stream()
                .map(EntitySavingsStats::getEntityId)
                .collect(Collectors.toSet())));
    }

    private EntityState createEntityState(long entityId, Double realizedSavings, Double realizedInvestments,
                                          Double missedSavings, Double missedInvestments) {
        EntityState state = new EntityState(entityId, SavingsUtil.EMPTY_PRICE_CHANGE);
        if (realizedSavings != null) {
            state.setRealizedSavings(realizedSavings);
        }
        if (realizedInvestments != null) {
            state.setRealizedInvestments(realizedInvestments);
        }
        if (missedSavings != null) {
            state.setMissedSavings(missedSavings);
        }
        if (missedInvestments != null) {
            state.setMissedInvestments(missedInvestments);
        }
        return state;
    }

    private static long getTimestamp(int hour, int min) {
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, min);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }
}
