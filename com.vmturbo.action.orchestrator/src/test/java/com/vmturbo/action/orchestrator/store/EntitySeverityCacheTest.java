package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache.OrderOidBySeverity;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache.SeverityCount;
import com.vmturbo.action.orchestrator.store.query.MapBackedActionViews;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit Tests for EntitySeverityCache.
 */
public class EntitySeverityCacheTest {

    private EntitySeverityCache entitySeverityCache;
    private ActionStore actionStore = mock(ActionStore.class);
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final ActionFactory actionFactory = new ActionFactory(actionModeCalculator);
    private static final long DEFAULT_SOURCE_ID = 1;
    private static final long ACTION_PLAN_ID = 9876;

    private final RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(
        repositoryServiceMole);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        entitySeverityCache = new EntitySeverityCache(
            RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            true);
    }

    @Test
    public void testRefreshVisibleNotDisabled() {
        ActionView action = actionView(executableMove(
            0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.CRITICAL));
        when(actionStore.getActionViews())
            .thenReturn(new MapBackedActionViews(
                Collections.singletonMap(action.getRecommendation().getId(), action)));

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testRefreshNotVisibleDisabled() {
        ActionView action = spy(actionView(executableMove(
            0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.CRITICAL)));
        doReturn(ActionMode.DISABLED).when(action).getMode();

        MapBackedActionViews actionViews = new MapBackedActionViews(
            ImmutableMap.of(action.getRecommendation().getId(), action),
            LiveActionStore.VISIBILITY_PREDICATE);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        entitySeverityCache.refresh(actionStore);
        assertEquals(Optional.empty(), entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID));
    }

    @Test
    public void testRefreshPicksMaxSeverity() {
        ActionView action1 = actionView(executableMove(
            0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.MINOR));
        ActionView action2 = actionView(executableMove(
            3, DEFAULT_SOURCE_ID, 1, 4, 1, Severity.CRITICAL));
        ActionView action3 = actionView(executableMove(
            5, DEFAULT_SOURCE_ID, 1, 6, 1, Severity.MAJOR));

        when(actionStore.getActionViews()).thenReturn(new MapBackedActionViews(ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3)));

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testRefreshIndividualAction() {
        ActionView action1 = actionView(executableMove(
            0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.MINOR));
        ActionView action2 = spy(actionView(executableMove(
            3, DEFAULT_SOURCE_ID, 1, 4, 1, Severity.CRITICAL)));
        ActionView action3 = actionView(executableMove(
            5, DEFAULT_SOURCE_ID, 1, 6, 1, Severity.MAJOR));
        ActionView unrelatedAction = actionView(executableMove(
            5, 999, 1, 6, 1, Severity.CRITICAL));

        final QueryableActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3,
            unrelatedAction.getRecommendation().getId(), unrelatedAction));
        when(actionStore.getActionViews()).thenReturn(actionViews);

        when(actionStore.getActionView(Mockito.anyLong()))
            .thenAnswer((Answer<Optional<ActionView>>)invocation -> {
                Long id = invocation.getArgumentAt(0, Long.class);
                return actionViews.get(id);
            });

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());

        // Simulate changing the state of the critical action to IN_PROGRESS and refreshing for that
        // action. State should the next most critical action that applies to the entity.
        when(action2.getState()).thenReturn(ActionState.IN_PROGRESS);

        entitySeverityCache.refresh(action2.getRecommendation(), actionStore);
        assertEquals(Severity.MAJOR, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    /**
     * VMs, Hosts, etc should only count actions where they are directly the source.
     * BusinessApp should count actions in entities below them in the supply chain.
     * <pre>
     *                                               /------------\
     *                                              /             v
     *     BusinessApp1 -> BTx1 -> Srv1 -> App1 -> VM1 -> PM1 -> Storage1
     *                                            Minor  Minor    Major
     *                                               /------------\
     *                                              /             v
     *     BusinessApp2 -> BTx2 -> Srv2 -> DB1 -> VM2 -> PM2 -> Storage2
     *                                   Critical               Major
     *                                                          Minor
     * BusinessTxn ends up using same host:
     *                                   /--------------\
     *                                  /                \
     *      --> Service1 --> App1 --> VM1 ---             \
     *     /                         Minor   \            V
     * BTxn3                                  --> PM1 -> Storage1
     *     \                                 /    Minor   Major
     *      --> Service3 --> App2 --> VM3 ---              ^
     *                                  \                  |
     *                                   \________________/
     *
     * Service can have multiple App/DB:
     *                           /--------------\
     *                          /                \
     * Service4 ----> App1 -> VM1----             \
     *           \            Minor  \             \
     *           \--> App2 -> VM3--   \            |
     *                         \    \   \           v
     *                          \    ------> PM1 -> Storage1
     *                           \          Minor   Major
     *                            \                  ^
     *                             \_________________/
     * </pre>
     * Virtual Volume is not in producer relationship and Application can have database as
     * a producer:
     * <pre>
     *            Service5
     *                |
     *                v
     *        Application Component5
     *       /            |
     *       v            |
     * Database4          |
     *      |             |
     *      |             |
     *      v             v
     *    VM4 --         VM5 - -
     *     | \ \          |   \ \
     *     v  \  \        v    \ \
     * Volume4 \  \   Volume5   \ \
     *     |   |   \ /           \ \
     *     |   |    o---         | \
     *     |   |   /    \        / |
     *     v   v  v     v       /  |
     *     Storage4 <- Host4 <--  /
     *     Major ^    Minor      /
     *            \_____________/
     * </pre>
     */
    @Test
    public void testSeverityBreakdownAndSeverityCounts() {
        SeverityBreakdownScenario severityBreakdownScenario = new SeverityBreakdownScenario(
            actionStore, repositoryServiceMole);

        // trigger the recomputation of severity and severity breakdown
        entitySeverityCache.refresh(actionStore);

        checkSupplyChainSeverityCount(Arrays.asList(
            severityBreakdownScenario.virtualMachine1Oid,
            severityBreakdownScenario.physicalMachine1Oid,
            severityBreakdownScenario.storage1Oid,
            severityBreakdownScenario.missingId),
            ImmutableMap.of(
                Optional.of(Severity.MINOR), 2L,
                Optional.of(Severity.MAJOR), 1L,
                Optional.empty(), 1L));

        // Missing entity should have no breaking.
        Assert.assertFalse(
            entitySeverityCache.getSeverityBreakdown(severityBreakdownScenario.missingId)
                .isPresent());

        // BusinessApp1 -> BTx -> Srv -> App -> VM1 -> PM1 -> Storage1
        //                                      Minor  Minor    Major
        // Minor: 2
        // Major: 1
        // Normal: 1
        // BusinessApp1, BusinessTx1, Service1 should have the same counts except NORMAL
        // BusinessApp1
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.businessApp1Oid,
            ImmutableMap.of(
                Severity.MINOR, 2,
                Severity.MAJOR, 1,
                Severity.NORMAL, 1));

        // BusinessTx1
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.businessTx1Oid,
            ImmutableMap.of(
                Severity.MINOR, 2,
                Severity.MAJOR, 1,
                Severity.NORMAL, 1));

        // Service1
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.service1Oid,
            ImmutableMap.of(
                Severity.MINOR, 2,
                Severity.MAJOR, 1,
                Severity.NORMAL, 1));

        // BusinessApp2 -> BTx -> Srv -> DB -> VM2 -> PM2 -> Storage2
        //                            Critical               Major
        //                                                   Minor
        // Critical: 1
        // Major: 1
        // Normal: 2
        // no minor because Storage2's max severity is major
        // BusinessApp2, BusinessTx2, Service3 should have the same counts
        // BusinessApp2
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.businessApp2Oid,
            ImmutableMap.of(
                Severity.CRITICAL, 1,
                Severity.MAJOR, 1,
                Severity.NORMAL, 2));

        // BusinessTx2
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.businessTx2Oid,
            ImmutableMap.of(
                Severity.CRITICAL, 1,
                Severity.MAJOR, 1,
                Severity.NORMAL, 2));

        // Service2
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.service2Oid,
            ImmutableMap.of(
                Severity.CRITICAL, 1,
                Severity.MAJOR, 1,
                Severity.NORMAL, 2));

        // BusinessTxn ends up using same host:
        //      --> Service1 --> App1 --> VM1 ---------------\
        //     /                         Minor   \            V
        // BTxn3                                  --> PM1 -> Storage1
        //     \                                 /    Minor   Major  ^
        //      --> Service3 --> App2 --> VM3 -----------------------/
        // PM1's Minor is counted twice. Once through App1 and again through App2.
        // The same goes for Storage1.
        checkSeverityBreakdown(severityBreakdownScenario.businessTx3Oid,
            ImmutableMap.of(
                Severity.MINOR, 3,
                Severity.MAJOR, 2,
                Severity.NORMAL, 3));

        // Service can have multiple App/DB:
        // Service4 ----> App1 -> VM1----
        //           \            Minor  \
        //           \--> App2 -> VM3--   \
        //                            \   \
        //                              ------> PM1 -> Storage1
        //                                     Minor   Major
        // PM1's Minor is counted twice. Once through App1 and again through App2.
        // The same goes for Storage1.
        checkSeverityBreakdown(severityBreakdownScenario.service4Oid,
            ImmutableMap.of(
                Severity.MINOR, 3,
                Severity.MAJOR, 2,
                Severity.NORMAL, 3));

        checkSeverityBreakdown(severityBreakdownScenario.service5Oid,
            ImmutableMap.of(
                Severity.MINOR, 2,
                Severity.MAJOR, 2,
                Severity.NORMAL, 6));
    }

    /**
     * When severity breakdown is disabled, getSeverityBreakdown should always return empty
     * and getSeverityCounts should not consider severity breakdowns.
     */
    @Test
    public void testSeverityBreakdownDisabled() {
        entitySeverityCache = new EntitySeverityCache(
            RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            false);

        SeverityBreakdownScenario severityBreakdownScenario = new SeverityBreakdownScenario(
            actionStore, repositoryServiceMole);

        // trigger the recomputation of severity and severity breakdown
        entitySeverityCache.refresh(actionStore);

        // severity breakdowns are disabled, so there input that returns a breakdown in the above
        // test should return empty map here.
        Map<Optional<Severity>, Long> actualMap =
            entitySeverityCache.getSeverityCounts(
                Collections.singletonList(severityBreakdownScenario.service4Oid));
        Assert.assertEquals(1, actualMap.size());
        Assert.assertEquals(Long.valueOf(1L), actualMap.get(Optional.empty()));
        Assert.assertEquals(Optional.empty(),
            entitySeverityCache.getSeverityBreakdown(severityBreakdownScenario.service4Oid));
    }

    private void checkBothSupplyChainSeverityCountAndBreakdown(
        long oid,
        Map<Severity, Integer> expectedSeveritiesAndCounts) {
        checkSeverityBreakdown(oid, expectedSeveritiesAndCounts);

        checkSupplyChainSeverityCount(
            Arrays.asList(oid),
            expectedSeveritiesAndCounts.entrySet().stream().collect(Collectors.toMap(
                entry -> Optional.of(entry.getKey()),
                entry -> entry.getValue().longValue())));
    }

    private void checkSupplyChainSeverityCount(
        @Nonnull List<Long> oidToSearchFor,
        @Nonnull Map<Optional<Severity>, Long> expectedSeveritiesAndCounts) {
        Map<Optional<Severity>, Long> actualSeverityCounts =
            entitySeverityCache.getSeverityCounts(oidToSearchFor);
        assertEquals(expectedSeveritiesAndCounts.size(), actualSeverityCounts.size());
        expectedSeveritiesAndCounts.forEach((expectedSeverity, expectedCount) -> {
            assertEquals(expectedCount, actualSeverityCounts.get(expectedSeverity));
        });
    }

    private void checkSeverityBreakdown(
        long oid,
        Map<Severity, Integer> expectedSeveritiesAndCounts) {
        Optional<SeverityCount> actualBreakdownOptional =
            entitySeverityCache.getSeverityBreakdown(oid);
        Assert.assertTrue(actualBreakdownOptional.isPresent());
        SeverityCount actualBreakdown = actualBreakdownOptional.get();

        assertEquals(expectedSeveritiesAndCounts.size(), actualBreakdown.getSeverityCounts().size());
        expectedSeveritiesAndCounts.forEach((expectedSeverity, expectedCount) -> {
            assertEquals("expected: " + expectedSeveritiesAndCounts
                + " actual: " + actualBreakdownOptional,
                expectedCount, actualBreakdown.getCountOfSeverity(expectedSeverity));
        });
    }

    /**
     * Holds all the information for the SeverityBreakdownScenario. Had to move this out of
     * {@link #testSeverityBreakdownAndSeverityCounts()}, because the method became longer than the
     * 256 line limit.
     */
    private class SeverityBreakdownScenario {
        // manually assign the oids to make it easier to debug
        public final long storage1Oid = 1001L;
        public final long storage2Oid = 1002L;
        public final long storage4Oid = 1004L;
        public final long physicalMachine1Oid = 2001L;
        public final long physicalMachine2Oid = 2002L;
        public final long physicalMachine4Oid = 2004L;
        public final long virtualMachine1Oid = 3001L;
        public final long virtualMachine2Oid = 3002L;
        public final long virtualMachine3Oid = 3003L;
        public final long virtualMachine4Oid = 3004L;
        public final long virtualMachine5Oid = 3005L;
        public final long missingId = 9999L;
        public final long application1Oid = 4001L;
        public final long application2Oid = 4002L;
        public final long application5Oid = 4005L;
        public final long database1Oid = 5001L;
        public final long database4Oid = 5004L;
        public final long service1Oid = 6001L;
        public final long service2Oid = 6002L;
        public final long service3Oid = 6003L;
        public final long service4Oid = 6004L;
        public final long service5Oid = 6005L;
        public final long businessTx1Oid = 7001L;
        public final long businessTx2Oid = 7002L;
        public final long businessTx3Oid = 7003L;
        public final long businessApp1Oid = 8001L;
        public final long businessApp2Oid = 8002L;
        public final long virtualVolume4Oid = 9004L;
        public final long virtualVolume5Oid = 9005L;

        /**
         * Sets up actionStore, and repositoryServiceMole with the scenario.
         *
         * @param actionStore Sets up actionStore with the scenario.
         * @param repositoryServiceMole Sets up repositoryServiceMole with the scenario.
         */
        private SeverityBreakdownScenario(
            ActionStore actionStore,
            RepositoryServiceMole repositoryServiceMole) {
            List<ActionView> actions = Arrays.asList(
                actionView(executableMove(0, virtualMachine1Oid, 1, 2, 1, Severity.MINOR)),
                actionView(executableMove(3, physicalMachine1Oid, 1, 4, 1, Severity.MINOR)),
                actionView(executableMove(5, storage1Oid, 1, 6, 1, Severity.MAJOR)),
                actionView(executableMove(5, database1Oid, 1, 2, 1, Severity.CRITICAL)),
                actionView(executableMove(5, storage2Oid, 1, 2, 1, Severity.MINOR)),
                actionView(executableMove(5, storage2Oid, 1, 2, 1, Severity.MAJOR)),
                actionView(executableMove(5, storage4Oid, 1, 2, 1, Severity.MAJOR)),
                actionView(executableMove(5, physicalMachine4Oid, 1, 2, 1, Severity.MINOR))
            );

            // refresh cache using the above actions
            when(actionStore.getActionViews()).thenReturn(new MapBackedActionViews(
                actions.stream().collect(
                    Collectors.toMap(
                        actionView -> actionView.getRecommendation().getId(),
                        Function.identity()))));

            when(repositoryServiceMole.retrieveTopologyEntities(any()))
                .thenReturn(Collections.emptyList());

            setupRetrieveTopologyEntities(EntityType.BUSINESS_APPLICATION,
                makeApiPartialEntity(businessApp1Oid, businessTx1Oid),
                makeApiPartialEntity(businessApp2Oid, businessTx2Oid));

            setupRetrieveTopologyEntities(EntityType.BUSINESS_TRANSACTION,
                makeApiPartialEntity(businessTx1Oid, service1Oid),
                makeApiPartialEntity(businessTx2Oid, service2Oid),
                makeApiPartialEntity(businessTx3Oid, service1Oid, service3Oid));

            setupRetrieveTopologyEntities(EntityType.SERVICE,
                makeApiPartialEntity(service1Oid, application1Oid),
                makeApiPartialEntity(service2Oid, database1Oid),
                makeApiPartialEntity(service3Oid, application2Oid),
                makeApiPartialEntity(service4Oid, application1Oid, application2Oid),
                makeApiPartialEntity(service5Oid, application5Oid));

            setupRetrieveTopologyEntities(EntityType.APPLICATION_COMPONENT,
                makeApiPartialEntity(application1Oid, virtualMachine1Oid),
                makeApiPartialEntity(application2Oid, virtualMachine3Oid),
                makeApiPartialEntity(application5Oid, virtualMachine5Oid, database4Oid));

            setupRetrieveTopologyEntities(EntityType.DATABASE,
                makeApiPartialEntity(database1Oid, virtualMachine2Oid),
                makeApiPartialEntity(database4Oid, virtualMachine4Oid));

            setupRetrieveTopologyEntities(EntityType.VIRTUAL_MACHINE,
                makeApiPartialEntity(virtualMachine1Oid, physicalMachine1Oid, storage1Oid),
                makeApiPartialEntity(virtualMachine2Oid, physicalMachine2Oid, storage2Oid),
                makeApiPartialEntity(virtualMachine3Oid, physicalMachine1Oid, storage1Oid),
                makeApiPartialEntity(virtualMachine4Oid,
                    ImmutableList.of(
                        makeRelatedEntity(EntityType.VIRTUAL_VOLUME, virtualVolume4Oid),
                        makeRelatedEntity(EntityType.STORAGE, storage4Oid)),
                    physicalMachine4Oid, storage4Oid),
                makeApiPartialEntity(virtualMachine5Oid,
                    ImmutableList.of(
                        makeRelatedEntity(EntityType.VIRTUAL_VOLUME, virtualVolume5Oid),
                        makeRelatedEntity(EntityType.STORAGE, storage4Oid)),
                    physicalMachine4Oid, storage4Oid));

            setupRetrieveTopologyEntities(EntityType.VIRTUAL_VOLUME,
                makeApiPartialEntity(virtualVolume4Oid, storage4Oid),
                makeApiPartialEntity(virtualVolume5Oid, storage4Oid));

            setupRetrieveTopologyEntities(EntityType.PHYSICAL_MACHINE,
                makeApiPartialEntity(physicalMachine1Oid, storage1Oid),
                makeApiPartialEntity(physicalMachine2Oid, storage2Oid),
                makeApiPartialEntity(physicalMachine4Oid, storage4Oid));

            setupRetrieveTopologyEntities(EntityType.STORAGE,
                makeApiPartialEntity(storage1Oid),
                makeApiPartialEntity(storage2Oid),
                makeApiPartialEntity(storage4Oid));
        }


    }

    private void setupRetrieveTopologyEntities(EntityType entityType,
                                               PartialEntity... partialEntities) {
        PartialEntityBatch.Builder partialEntityBatch = PartialEntityBatch.newBuilder();
        for (PartialEntity partialEntity : partialEntities) {
            partialEntityBatch.addEntities(partialEntity);
        }

        when(repositoryServiceMole.retrieveTopologyEntities(
            RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityType(entityType.getNumber())
                .setReturnType(Type.API)
                .build()))
            .thenReturn(Arrays.asList(partialEntityBatch.buildPartial()));
    }

    private RelatedEntity makeRelatedEntity(EntityType entityType, long oid) {
        return RelatedEntity.newBuilder()
            .setOid(oid)
            .setEntityType(entityType.getNumber())
            .buildPartial();
    }

    @Nonnull
    private PartialEntity makeApiPartialEntity(long entityOid, long... providerOids) {
        return makeApiPartialEntity(entityOid, Collections.emptyList(), providerOids);
    }

    @Nonnull
    private PartialEntity makeApiPartialEntity(long entityOid,
                                                           List<RelatedEntity> connectedEntities,
                                                           long... providerOids) {
        ApiPartialEntity.Builder apiPartialEntity = ApiPartialEntity.newBuilder()
                .setOid(entityOid)
                .addAllConnectedTo(connectedEntities)
                .addAllProviders(Arrays.stream(providerOids)
                    .boxed()
                    .map(providerOid -> RelatedEntity.newBuilder()
                        .setOid(providerOid)
                        .buildPartial())
                    .collect(Collectors.toList()));

        return PartialEntity.newBuilder().setApi(apiPartialEntity.buildPartial()).buildPartial();
    }

    /**
     * The comparator should result in the below ascending order.
     * 1. An entity without severity and without severity breakdown
     * 2. An entity with severity but without severity breakdown
     * 3. An entity with severity but with empty severity breakdown map
     * 4. An entity with lowest severity break down
     * 5. An entity with same proportion, but a higher count of that severity
     * 6. An entity with the same highest severity, but the proportion is higher
     * 7. An entity with a higher severity in the breakdown.
     * 8. An entity with an even higher severity in the breakdown but the entity does not have a
     *    a severity (edge case).
     */
    @Test
    public void testBreakdownOrderBy() {
        long id = 0;
        EntitySeverityCache mockedEntitySeverityCache = spy(entitySeverityCache);
        // both not found
        long bothNotFound = id++;
        when(mockedEntitySeverityCache.getSeverity(bothNotFound)).thenReturn(Optional.empty());
        when(mockedEntitySeverityCache.getSeverityBreakdown(bothNotFound)).thenReturn(Optional.empty());
        // only has entity level severity
        long onlyHasEntityLevelSeverity = id++;
        when(mockedEntitySeverityCache.getSeverity(onlyHasEntityLevelSeverity)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(onlyHasEntityLevelSeverity)).thenReturn(Optional.empty());
        // only has entity level severity, and empty severity breakdown
        long emptyBreakdown = id++;
        when(mockedEntitySeverityCache.getSeverity(emptyBreakdown)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(emptyBreakdown)).thenReturn(Optional.of(new SeverityCount()));
        // has entity level severity, and lowest severity
        long lowestSeverityBreakdown = id++;
        when(mockedEntitySeverityCache.getSeverity(lowestSeverityBreakdown)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(lowestSeverityBreakdown)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 2,
                Severity.NORMAL, 2)
        )));
        // exact same severity as previous should be tied
        long exactSameSeverity = 99L;
        when(mockedEntitySeverityCache.getSeverity(exactSameSeverity)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(exactSameSeverity)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 2,
                Severity.NORMAL, 2)
        )));
        // same proportion but higher count
        long sameProportionHigherCount = id++;
        when(mockedEntitySeverityCache.getSeverity(sameProportionHigherCount)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(sameProportionHigherCount)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 4,
                Severity.NORMAL, 4)
        )));
        // higher proportion
        long higherProportion = id++;
        when(mockedEntitySeverityCache.getSeverity(higherProportion)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(higherProportion)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 3,
                Severity.NORMAL, 2)
        )));
        // higher severity
        long higherSeverity = id++;
        when(mockedEntitySeverityCache.getSeverity(higherSeverity)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(higherSeverity)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 2,
                Severity.NORMAL, 2,
                Severity.MAJOR, 2)
        )));
        // even higher severity, but no entity level severity
        long evenHigherSeverityButNoEntitySeverity = id++;
        when(mockedEntitySeverityCache.getSeverity(evenHigherSeverityButNoEntitySeverity)).thenReturn(Optional.empty());
        when(mockedEntitySeverityCache.getSeverityBreakdown(evenHigherSeverityButNoEntitySeverity)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 2,
                Severity.NORMAL, 2,
                Severity.CRITICAL, 2)
        )));

        List<Long> ascendingOrder = new ArrayList<>();
        for (long i = 0; i < id; i++) {
            ascendingOrder.add(i);
        }
        List<Long> descendingOrder = new ArrayList<>(ascendingOrder);
        Collections.reverse(descendingOrder);
        List<Long> randomOrder = new ArrayList<>(ascendingOrder);
        Collections.shuffle(randomOrder);

        Comparator<Long> desc = new OrderOidBySeverity(mockedEntitySeverityCache).reversed();
        Assert.assertEquals(0, desc.compare(lowestSeverityBreakdown, exactSameSeverity));

        checkSortedByComparator(ascendingOrder, desc, descendingOrder);
        checkSortedByComparator(randomOrder, desc, descendingOrder);
        Comparator<Long> asc = new OrderOidBySeverity(mockedEntitySeverityCache);
        checkSortedByComparator(descendingOrder, asc, ascendingOrder);
        checkSortedByComparator(randomOrder, asc, ascendingOrder);
    }

    private SeverityCount makeSeverityCount(Map<Severity, Integer> severityBreakdown) {
        SeverityCount severityCount = new SeverityCount();
        for (Map.Entry<Severity, Integer> entry : severityBreakdown.entrySet()) {
            severityCount.addSeverity(entry.getKey(), entry.getValue());
        }
        return severityCount;
    }

    private void checkSortedByComparator(
        @Nonnull List<Long> thingsToSort,
        @Nonnull Comparator<Long> comparator,
        @Nonnull List<Long> expectedOrder) {
        List<Long> actual = new ArrayList<>(thingsToSort);
        Collections.sort(actual, comparator);
        Assert.assertEquals(expectedOrder, actual);
    }

    @Nonnull
    private ActionView actionView(ActionDTO.Action recommendation) {
        return actionFactory.newAction(recommendation, ACTION_PLAN_ID, IdentityGenerator.next());
    }

    private static ActionDTO.Action executableMove(final long targetId,
                                                   final long sourceId, int sourceType,
                                                   final long destinationId, int destinationType,
                                                   Severity severity) {
        return ActionDTO.Action.newBuilder()
            .setExecutable(true)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setId(IdentityGenerator.next())
            .setDeprecatedImportance(mapSeverityToImportance(severity))
            .setInfo(
                TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType,
                    destinationId, destinationType))
            .setExplanation(mapSeverityToExplanation(severity))
            .build();
    }

    private static double mapSeverityToImportance(Severity severity) {
        switch (severity) {
            case NORMAL:
                return ActionDTOUtil.NORMAL_SEVERITY_THRESHOLD - 1.0;
            case MINOR:
                return ActionDTOUtil.MINOR_SEVERITY_THRESHOLD - 1.0;
            case MAJOR:
                return ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD - 1.0;
            case CRITICAL:
                return ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD + 1.0;
            default:
                throw new IllegalArgumentException("Unknown severity " + severity);
        }
    }

    /**
     * Map the severity category to one of the Explanations of that type of severity.
     *
     * @param severity The name of the severity category.
     * @return The Explanation which can be used to generate severity category.
     */
    private static Explanation mapSeverityToExplanation(Severity severity) {
        switch (severity) {
            case NORMAL:
                return Explanation.newBuilder().setMove(
                    MoveExplanation.newBuilder().addChangeProviderExplanation(
                        ChangeProviderExplanation.newBuilder().setInitialPlacement(
                            ChangeProviderExplanation.InitialPlacement.getDefaultInstance())
                            .build())
                        .build())
                    .build();
            case MINOR:
                return Explanation.newBuilder().setMove(
                    MoveExplanation.newBuilder().addChangeProviderExplanation(
                        ChangeProviderExplanation.newBuilder().setEfficiency(
                            ChangeProviderExplanation.Efficiency.getDefaultInstance())
                            .build())
                        .build())
                    .build();
            case MAJOR:
                return Explanation.newBuilder().setMove(
                    MoveExplanation.newBuilder().addChangeProviderExplanation(
                        ChangeProviderExplanation.newBuilder().setPerformance(
                            ChangeProviderExplanation.Performance.getDefaultInstance())
                            .build())
                        .build())
                    .build();
            case CRITICAL:
                return Explanation.newBuilder().setMove(
                    MoveExplanation.newBuilder().addChangeProviderExplanation(
                        ChangeProviderExplanation.newBuilder().setCongestion(
                            ChangeProviderExplanation.Congestion.getDefaultInstance())
                            .build())
                        .build())
                    .build();
            default:
                throw new IllegalArgumentException("Unknown severity " + severity);
        }
    }
}
