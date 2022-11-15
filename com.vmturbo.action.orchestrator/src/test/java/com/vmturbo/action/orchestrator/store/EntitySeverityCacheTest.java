package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.api.EntitySeverityNotificationSender;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache.SeverityCount;
import com.vmturbo.action.orchestrator.store.query.MapBackedActionViews;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraphCreator;

/**
 * Unit Tests for EntitySeverityCache.
 */
public class EntitySeverityCacheTest {

    private EntitySeverityCache entitySeverityCache;
    private ActionStore actionStore = mock(ActionStore.class);
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final ActionFactory actionFactory = new ActionFactory(actionModeCalculator,
            Collections.emptyList());
    private static final long DEFAULT_SOURCE_ID = 1;
    private static final long ACTION_PLAN_ID = 9876;

    private ActionTopologyStore actionTopologyStore = mock(ActionTopologyStore.class);
    private EntitySeverityNotificationSender notificationSender = mock(EntitySeverityNotificationSender.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        entitySeverityCache = new EntitySeverityCache(actionTopologyStore, notificationSender, true);

        ActionRealtimeTopology actionRealtimeTopology = mock(ActionRealtimeTopology.class);
        when(actionRealtimeTopology.entityGraph())
            .thenReturn(new TopologyGraphCreator<ActionGraphEntity.Builder, ActionGraphEntity>().build());
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.of(actionRealtimeTopology));
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
        assertEquals(Optional.of(Severity.NORMAL), entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID));
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

    /**
     * Create a move without any changes. This will cause an IndexOutOfBounds exception in
     * ActionDTOUtil.getSeverityEntity.
     * But inspite of this, the cache refresh should continue with other entities.
     */
    @Test
    public void testRefreshContinuesOnException() {
        ActionDTO.Action.Builder moveBuilder = executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1,
                Severity.MINOR).toBuilder();
        moveBuilder.getInfoBuilder().getMoveBuilder().removeChanges(0).build();
        ActionDTO.Action moveWithNoChanges = moveBuilder.build();
        ActionView action1 = actionView(moveWithNoChanges);
        ActionView action2 = actionView(executableMove(
                3, 2, 1, 4, 1, Severity.CRITICAL));
        ActionView action3 = actionView(executableMove(
                5, 2, 1, 6, 1, Severity.MAJOR));

        when(actionStore.getActionViews()).thenReturn(new MapBackedActionViews(ImmutableMap.of(
                action1.getRecommendation().getId(), action1,
                action2.getRecommendation().getId(), action2,
                action3.getRecommendation().getId(), action3)));

        entitySeverityCache.refresh(actionStore);
        // For the action which causes the exception, the associated entity's severity should just be the default - Normal.
        assertEquals(Severity.NORMAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
        // The other actions should still be processed, and we should get severity for them.
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(2).get());
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
     * Service is supported by containers and pods which are also aggregated to Namespace:
     * <pre>
     *
     *                                +------------------------------+
     *                                |                              |
     *                                |             -> VM1           |
     *                                |            /                 |
     *                                |        (Minor)               |
     * Service6 ---> App6a1 -> Container6a1 --> Pod6a ----------------------------------+
     *           \  (Minor)       (Minor)         ^                  |                  |
     *           \                                |      (Major)     |                  |
     *           \-> App6a2 -> Container6a2 ------+-----> Spec62 -----------------+     |
     *           \                                          ^        |            |     |
     *           \                                          |        V            V     V
     *           \-> App6b1 -> Container6b1 ------+--------------> Spec61 -> WorkloadController6 -> NamespaceFoo
     *           \                                |         |      (Minor)   (Minor) ^  (Major)
     *           \                                V         |                        |
     *           \-> App6b2 -> Container6b2 --> Pod6b -------------------------------+
     *                                 |       (Major)      |
     *                                 |          \         |
     *                                 |          \-> VM5   |
     *                                 |                    |
     *                                 +--------------------+
     * </pre>
     *
     * <p>Traversal over Container area towards Service is as follows:
     * <pre>
     *
     *  Container           ContainerSpec
     *      ^                     ^
     *      |                     |
     * ContainerPod <--- WorkloadController
     *                            ^
     *                            |
     *                        Namespace
     *
     * </pre>
     * Note: No traversal/accumulation between ContainerSpec and WorkloadController to
     * avoid double counting; they essentially have the same actions: the
     * WorkloadController's actions are the aggregates of the ContainerSpec's.
     *
     * <p>On the other hand, traversal across the Container area towards the Namespace is as
     * follows:
     * <pre>
     *
     *   Service
     *      |
     *      V
     * AppComponent
     *      |
     *      V
     *  Container   X X X   ContainerSpec
     *      |                     |
     *      V                     V
     * ContainerPod ---> WorkloadController
     *                            |
     *                            V
     *                        Namespace
     *
     * </pre>
     * Note: No traversal/accumulation between Container and ContainerSpec to
     * avoid double counting.
     */
    @Test
    public void testSeverityBreakdownAndSeverityCounts() {
        SeverityBreakdownScenario severityBreakdownScenario = new SeverityBreakdownScenario(
            actionStore, actionTopologyStore);

        // trigger the recomputation of severity and severity breakdown
        entitySeverityCache.refresh(actionStore);

        checkSupplyChainSeverityCount(Arrays.asList(
            severityBreakdownScenario.virtualMachine1Oid,
            severityBreakdownScenario.physicalMachine1Oid,
            severityBreakdownScenario.storage1Oid,
            severityBreakdownScenario.missingId),
            ImmutableMap.of(
                Severity.MINOR, 2L,
                Severity.MAJOR, 1L,
                Severity.NORMAL, 1L));

        // The cache does not distinguish between entities that do not exist and entities that
        // simply do not have actions/breakdowns.
        Assert.assertEquals(Collections.singletonMap(Severity.NORMAL, 1L),
                entitySeverityCache.getSeverityBreakdown(severityBreakdownScenario.missingId));

        // BusinessApp1 -> BTx -> Srv -> App -> VM1 -> PM1 -> Storage1
        //                                      Minor  Minor    Major
        // Minor: 2
        // Major: 1
        // Normal: 1
        // BusinessApp1, BusinessTx1, Service1 should have the same counts except NORMAL
        // BusinessApp1
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.businessApp1Oid,
            ImmutableMap.of(
                Severity.MINOR, 2L,
                Severity.MAJOR, 1L,
                Severity.NORMAL, 1L));

        // BusinessTx1
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.businessTx1Oid,
            ImmutableMap.of(
                Severity.MINOR, 2L,
                Severity.MAJOR, 1L,
                Severity.NORMAL, 1L));

        // Service1
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.service1Oid,
            ImmutableMap.of(
                Severity.MINOR, 2L,
                Severity.MAJOR, 1L,
                Severity.NORMAL, 1L));

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
                Severity.CRITICAL, 1L,
                Severity.MAJOR, 1L,
                Severity.NORMAL, 2L));

        // BusinessTx2
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.businessTx2Oid,
            ImmutableMap.of(
                Severity.CRITICAL, 1L,
                Severity.MAJOR, 1L,
                Severity.NORMAL, 2L));

        // Service2
        checkBothSupplyChainSeverityCountAndBreakdown(severityBreakdownScenario.service2Oid,
            ImmutableMap.of(
                Severity.CRITICAL, 1L,
                Severity.MAJOR, 1L,
                Severity.NORMAL, 2L));

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
                Severity.MINOR, 3L,
                Severity.MAJOR, 2L,
                Severity.NORMAL, 3L));

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
                Severity.MINOR, 3L,
                Severity.MAJOR, 2L,
                Severity.NORMAL, 3L));

        checkSeverityBreakdown(severityBreakdownScenario.service5Oid,
            ImmutableMap.of(
                Severity.MINOR, 2L,
                Severity.MAJOR, 2L,
                Severity.NORMAL, 6L));

        // Service on containers
        //
        // Service6 ----> App6a1 -> Container6a1 -----> Pod6a ----> Workload6 ----> NamespaceFoo
        //           \    Minor        Minor            Minor   \     Major
        //           \                                           \--> VM1 ------------> PM1
        //           \                                                Minor     \       Minor
        //           \                                                          \---> Storage1
        //           \                                                                 Major
        //           \--> App6a2 -> Container6a2 -----> Pod6a ----> Workload6 ----> NamespaceFoo
        //           \                                  Minor   \     Major
        //           \                                           \--> VM1 ------------> PM1
        //           \                                                Minor     \       Minor
        //           \                                                          \---> Storage1
        //           \                                                                 Major
        //           \--> App6b1 -> Container6b1 -----> Pod6b ----> Workload6 ----> NamespaceFoo
        //           \                                  Major   \     Major
        //           \                                           \-> VM5 ------> Storage4
        //           \                                                     \       Major
        //           \                                                      \---> PM4
        //           \                                                       \   Minor
        //           \                                                        \-> Vol5
        //           \--> App6b2 -> Container6b2 -----> Pod6b ----> Workload6 ----> NamespaceFoo
        //                                              Major   \     Major
        //                                                       \-> VM5 ------> Storage4
        //                                                                 \       Major
        //                                                                  \---> PM4
        //                                                                   \   Minor
        //                                                                    \-> Vol5
        //
        checkSeverityBreakdown(severityBreakdownScenario.service6Oid,
                ImmutableMap.of(
                        Severity.MAJOR, 10L,
                        Severity.MINOR, 10L,
                        Severity.NORMAL, 14L));

        // Namespace of containers
        //
        // NamespaceFoo ----> Workload6 ----------> Pod6a -----> Container6a1 -> App6a1 -> Service6
        //                      Major     \         Minor   \       Minor        Minor
        //                                 \                 \-> Container6a2 -> App6a2 -> Service6
        //                                  \
        //                                   \----> Pod6b -----> Container6b1 -> App6b1 -> Service6
        //                                          Major   \
        //                                                   \-> Container6a2 -> App6a2 -> Service6
        //
        // Note: Namespace has "includeSelf" true; in the future it will have actions.
        //
        checkSeverityBreakdown(severityBreakdownScenario.namespaceFooOid,
                ImmutableMap.of(
                        Severity.MAJOR, 2L,
                        Severity.MINOR, 3L,
                        Severity.NORMAL, 11L));

        // ContainerSpecs (they are standalone)
        checkSeverityBreakdown(severityBreakdownScenario.containerSpec61Oid,
                ImmutableMap.of(Severity.MAJOR, 1L));
        checkSeverityBreakdown(severityBreakdownScenario.containerSpec62Oid,
                ImmutableMap.of(Severity.NORMAL, 1L));
        checkSupplyChainSeverityCount(Collections.singletonList(
                        severityBreakdownScenario.virtualMachineSpec3006id),
                ImmutableMap.of(
                        Severity.MINOR, 1L));
        checkSupplyChainSeverityCount(Collections.singletonList(
                        severityBreakdownScenario.appComponentSpec4605id),
                ImmutableMap.of(
                        Severity.MINOR, 1L));
    }

    /**
     * When severity breakdown is disabled, getSeverityBreakdown should always return empty
     * and getSeverityCounts should not consider severity breakdowns.
     */
    @Test
    public void testSeverityBreakdownDisabled() {
        entitySeverityCache = new EntitySeverityCache(
                actionTopologyStore, notificationSender, false);

        SeverityBreakdownScenario severityBreakdownScenario = new SeverityBreakdownScenario(
            actionStore, actionTopologyStore);

        // trigger the recomputation of severity and severity breakdown
        entitySeverityCache.refresh(actionStore);

        // severity breakdowns are disabled, so there input that returns a breakdown in the above
        // test should return normal severities.
        Map<Severity, Long> actualMap =
            entitySeverityCache.getSeverityCounts(
                Collections.singletonList(severityBreakdownScenario.service4Oid));
        Assert.assertEquals(1, actualMap.size());
        Assert.assertEquals(Collections.singletonMap(Severity.NORMAL, 1L), actualMap);
        Assert.assertEquals(Collections.singletonMap(Severity.NORMAL, 1L),
            entitySeverityCache.getSeverityBreakdown(severityBreakdownScenario.service4Oid));
    }

    private void checkBothSupplyChainSeverityCountAndBreakdown(
        long oid,
        Map<Severity, Long> expectedSeveritiesAndCounts) {
        checkSeverityBreakdown(oid, expectedSeveritiesAndCounts);

        checkSupplyChainSeverityCount(
            Arrays.asList(oid),
            expectedSeveritiesAndCounts.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey(),
                entry -> entry.getValue().longValue())));
    }

    private void checkSupplyChainSeverityCount(
        @Nonnull List<Long> oidToSearchFor,
        @Nonnull Map<Severity, Long> expectedSeveritiesAndCounts) {
        Map<Severity, Long> actualSeverityCounts =
            entitySeverityCache.getSeverityCounts(oidToSearchFor);
        assertEquals(expectedSeveritiesAndCounts.size(), actualSeverityCounts.size());
        expectedSeveritiesAndCounts.forEach((expectedSeverity, expectedCount) -> {
            assertEquals(expectedCount, actualSeverityCounts.get(expectedSeverity));
        });
    }

    private void checkSeverityBreakdown(
        long oid,
        Map<Severity, Long> expectedSeveritiesAndCounts) {
        Map<Severity, Long> actualBreakdown =
            entitySeverityCache.getSeverityBreakdown(oid);
        Assert.assertFalse(actualBreakdown.isEmpty());

        assertEquals(expectedSeveritiesAndCounts.size(), actualBreakdown.size());
        expectedSeveritiesAndCounts.forEach((expectedSeverity, expectedCount) -> {
            assertEquals("expected: " + expectedSeveritiesAndCounts
                + " actual: " + actualBreakdown,
                expectedCount, actualBreakdown.get(expectedSeverity));
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
        public final long container6a1Oid = 3401L;
        public final long container6a2Oid = 3402L;
        public final long container6b1Oid = 3403L;
        public final long container6b2Oid = 3404L;
        public final long containerPod6aOid = 3501L;
        public final long containerPod6bOid = 3502L;
        public final long containerSpec61Oid = 3601L;
        public final long containerSpec62Oid = 3602L;
        public final long workloadController6Oid = 3701L;
        public final long namespaceFooOid = 3801L;
        public final long application1Oid = 4001L;
        public final long application2Oid = 4002L;
        public final long application5Oid = 4005L;
        public final long application6a1Oid = 4601L;
        public final long application6a2Oid = 4602L;
        public final long application6b1Oid = 4603L;
        public final long application6b2Oid = 4604L;
        public final long database1Oid = 5001L;
        public final long database4Oid = 5004L;
        public final long service1Oid = 6001L;
        public final long service2Oid = 6002L;
        public final long service3Oid = 6003L;
        public final long service4Oid = 6004L;
        public final long service5Oid = 6005L;
        public final long service6Oid = 6006L;
        public final long businessTx1Oid = 7001L;
        public final long businessTx2Oid = 7002L;
        public final long businessTx3Oid = 7003L;
        public final long businessApp1Oid = 8001L;
        public final long businessApp2Oid = 8002L;
        public final long virtualVolume4Oid = 9004L;
        public final long virtualVolume5Oid = 9005L;
        public final long appComponentSpec4605id = 4605L;
        public final long virtualMachineSpec3006id = 3006L;
        public final long missingId = 9999L;

        /**
         * Sets up actionStore, and repositoryServiceMole with the scenario.
         *
         * @param actionStore Sets up actionStore with the scenario.
         * @param actionTopologyStore Sets up repositoryServiceMole with the scenario.
         */
        private SeverityBreakdownScenario(ActionStore actionStore,
                ActionTopologyStore actionTopologyStore) {
            List<ActionView> actions = Arrays.asList(
                actionView(executableMove(6, application6a1Oid, 1, 2, 1, Severity.MINOR)),
                actionView(executableMove(6, container6a1Oid, 1, 2, 1, Severity.MINOR)),
                actionView(executableMove(6, containerPod6aOid, 1, 2, 1, Severity.MINOR)),
                actionView(executableMove(6, containerPod6bOid, 1, 2, 1, Severity.MAJOR)),
                actionView(executableContainerAtomicResize(workloadController6Oid, containerSpec61Oid, Collections.singletonList(container6a1Oid), Severity.MAJOR)),
                actionView(executableMove(0, virtualMachine1Oid, 1, 2, 1, Severity.MINOR)),
                actionView(executableMove(3, physicalMachine1Oid, 1, 4, 1, Severity.MINOR)),
                actionView(executableMove(5, storage1Oid, 1, 6, 1, Severity.MAJOR)),
                actionView(executableMove(5, database1Oid, 1, 2, 1, Severity.CRITICAL)),
                actionView(executableMove(5, storage2Oid, 1, 2, 1, Severity.MINOR)),
                actionView(executableMove(5, storage2Oid, 1, 2, 1, Severity.MAJOR)),
                actionView(executableMove(5, storage4Oid, 1, 2, 1, Severity.MAJOR)),
                actionView(executableMove(5, physicalMachine4Oid, 1, 2, 1, Severity.MINOR)),
                actionView(executableMove(5, virtualMachineSpec3006id, 1, 2, 1, Severity.MINOR))
            );

            // refresh cache using the above actions
            when(actionStore.getActionViews()).thenReturn(new MapBackedActionViews(
                actions.stream().collect(
                    Collectors.toMap(
                        actionView -> actionView.getRecommendation().getId(),
                        Function.identity()))));

            TopologyGraphCreator<ActionGraphEntity.Builder, ActionGraphEntity> graphCreator =
                    new TopologyGraphCreator<>();
            makeEntity(EntityType.BUSINESS_APPLICATION, businessApp1Oid, graphCreator, businessTx1Oid);
            makeEntity(EntityType.BUSINESS_APPLICATION, businessApp2Oid, graphCreator,  businessTx2Oid);

            makeEntity(EntityType.BUSINESS_TRANSACTION, businessTx1Oid, graphCreator, service1Oid);
            makeEntity(EntityType.BUSINESS_TRANSACTION, businessTx2Oid, graphCreator, service2Oid);
            makeEntity(EntityType.BUSINESS_TRANSACTION, businessTx3Oid, graphCreator, service1Oid, service3Oid);

            makeEntity(EntityType.SERVICE, service1Oid, graphCreator, application1Oid);
            makeEntity(EntityType.SERVICE, service2Oid, graphCreator, database1Oid);
            makeEntity(EntityType.SERVICE, service3Oid, graphCreator, application2Oid);
            makeEntity(EntityType.SERVICE, service4Oid, graphCreator, application1Oid, application2Oid);
            makeEntity(EntityType.SERVICE, service5Oid, graphCreator, application5Oid);
            makeEntity(EntityType.SERVICE, service6Oid, graphCreator, application6a1Oid,
                    application6a2Oid, application6b1Oid, application6b2Oid);

            makeEntity(EntityType.APPLICATION_COMPONENT, application1Oid, graphCreator, virtualMachine1Oid);
            makeEntity(EntityType.APPLICATION_COMPONENT, application2Oid, graphCreator, virtualMachine3Oid);
            makeEntity(EntityType.APPLICATION_COMPONENT, application5Oid, graphCreator, virtualMachine5Oid, database4Oid);
            makeEntity(EntityType.APPLICATION_COMPONENT, application6a1Oid, graphCreator, container6a1Oid);
            makeEntity(EntityType.APPLICATION_COMPONENT, application6a2Oid, graphCreator, container6a2Oid);
            makeEntity(EntityType.APPLICATION_COMPONENT, application6b1Oid, graphCreator, container6b1Oid);
            makeEntity(EntityType.APPLICATION_COMPONENT, application6b2Oid, graphCreator, container6b2Oid);

            makeEntity(EntityType.CONTAINER, container6a1Oid, graphCreator, ImmutableList.of(
                    newConnectedEntity(ConnectionType.CONTROLLED_BY_CONNECTION, EntityType.CONTAINER_SPEC, containerSpec61Oid)),
                    containerPod6aOid);
            makeEntity(EntityType.CONTAINER, container6a2Oid, graphCreator, ImmutableList.of(
                    newConnectedEntity(ConnectionType.CONTROLLED_BY_CONNECTION, EntityType.CONTAINER_SPEC, containerSpec62Oid)),
                    containerPod6aOid);
            makeEntity(EntityType.CONTAINER, container6b1Oid, graphCreator, ImmutableList.of(
                    newConnectedEntity(ConnectionType.CONTROLLED_BY_CONNECTION, EntityType.CONTAINER_SPEC, containerSpec61Oid)),
                    containerPod6bOid);
            makeEntity(EntityType.CONTAINER, container6b2Oid, graphCreator, ImmutableList.of(
                    newConnectedEntity(ConnectionType.CONTROLLED_BY_CONNECTION, EntityType.CONTAINER_SPEC, containerSpec62Oid)),
                    containerPod6bOid);

            makeEntity(EntityType.CONTAINER_POD, containerPod6aOid, graphCreator, virtualMachine1Oid, workloadController6Oid);
            makeEntity(EntityType.CONTAINER_POD, containerPod6bOid, graphCreator, virtualMachine5Oid, workloadController6Oid);
            makeEntity(EntityType.CONTAINER_SPEC, containerSpec61Oid, graphCreator, workloadController6Oid);
            makeEntity(EntityType.CONTAINER_SPEC, containerSpec62Oid, graphCreator, workloadController6Oid);
            makeEntity(EntityType.WORKLOAD_CONTROLLER, workloadController6Oid, graphCreator, namespaceFooOid);
            makeEntity(EntityType.NAMESPACE, namespaceFooOid, graphCreator);

            makeEntity(EntityType.DATABASE, database1Oid, graphCreator, virtualMachine2Oid);
            makeEntity(EntityType.DATABASE, database4Oid, graphCreator, virtualMachine4Oid);

            makeEntity(EntityType.VIRTUAL_MACHINE, virtualMachine1Oid, graphCreator, physicalMachine1Oid, storage1Oid);
            makeEntity(EntityType.VIRTUAL_MACHINE, virtualMachine2Oid, graphCreator, physicalMachine2Oid, storage2Oid);
            makeEntity(EntityType.VIRTUAL_MACHINE, virtualMachine3Oid, graphCreator, physicalMachine1Oid, storage1Oid);
            makeEntity(EntityType.VIRTUAL_MACHINE, virtualMachine4Oid, graphCreator,
                    ImmutableList.of(
                            newConnectedEntity(ConnectionType.NORMAL_CONNECTION, EntityType.STORAGE_VOLUME, virtualVolume4Oid),
                            newConnectedEntity(ConnectionType.NORMAL_CONNECTION, EntityType.STORAGE, storage4Oid)),
                    physicalMachine4Oid, storage4Oid);
            makeEntity(EntityType.VIRTUAL_MACHINE, virtualMachine5Oid, graphCreator,
                    ImmutableList.of(
                            newConnectedEntity(ConnectionType.NORMAL_CONNECTION, EntityType.STORAGE_VOLUME, virtualVolume5Oid),
                            newConnectedEntity(ConnectionType.NORMAL_CONNECTION, EntityType.STORAGE, storage4Oid)),
                    physicalMachine4Oid, storage4Oid);

            makeEntity(EntityType.VIRTUAL_VOLUME, virtualVolume4Oid, graphCreator, storage4Oid);
            makeEntity(EntityType.VIRTUAL_VOLUME, virtualVolume5Oid, graphCreator, storage4Oid);

            makeEntity(EntityType.PHYSICAL_MACHINE, physicalMachine1Oid, graphCreator, storage1Oid);
            makeEntity(EntityType.PHYSICAL_MACHINE, physicalMachine2Oid, graphCreator, storage2Oid);
            makeEntity(EntityType.PHYSICAL_MACHINE, physicalMachine4Oid, graphCreator, storage4Oid);

            makeEntity(EntityType.STORAGE, storage1Oid, graphCreator);
            makeEntity(EntityType.STORAGE, storage2Oid, graphCreator);
            makeEntity(EntityType.STORAGE, storage4Oid, graphCreator);

            makeEntity(EntityType.VIRTUAL_MACHINE_SPEC, virtualMachineSpec3006id, graphCreator);
            makeEntity(EntityType.APPLICATION_COMPONENT_SPEC, appComponentSpec4605id, graphCreator);
            makeEntity(EntityType.APPLICATION_COMPONENT_SPEC, appComponentSpec4605id, graphCreator, ImmutableList.of(
                            newConnectedEntity(ConnectionType.NORMAL_CONNECTION, EntityType.VIRTUAL_MACHINE_SPEC,
                                    virtualMachineSpec3006id)));

            ActionRealtimeTopology actionRealtimeTopology = mock(ActionRealtimeTopology.class);
            when(actionRealtimeTopology.entityGraph())
                    .thenReturn(graphCreator.build());
            when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.of(actionRealtimeTopology));
        }

        private ActionGraphEntity.Builder makeEntity(EntityType type,
                long oid,
                TopologyGraphCreator<ActionGraphEntity.Builder, ActionGraphEntity> graphCreator,
                long... providerOids) {
            return makeEntity(type, oid, graphCreator, Collections.emptyList(), providerOids);
        }

        private ActionGraphEntity.Builder makeEntity(EntityType type,
                long oid,
                TopologyGraphCreator<ActionGraphEntity.Builder, ActionGraphEntity> graphCreator,
                List<ConnectedEntity> connectedEntities,
                long... providerOids) {
            TopologyEntityDTO.Builder e = TopologyEntityDTO.newBuilder()
                    .setOid(oid)
                    .setDisplayName(type.name() + "-" + oid)
                    .setEntityType(type.getNumber());
            for (long provider : providerOids) {
                e.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(provider));
            }

            e.addAllConnectedEntityList(connectedEntities);

            ActionGraphEntity.Builder bldr = new ActionGraphEntity.Builder(e.build());
            graphCreator.addEntity(bldr);
            return bldr;
        }

        private ConnectedEntity newConnectedEntity(ConnectionType connectionType,
                EntityType entityType, long id) {
            return ConnectedEntity.newBuilder()
                    .setConnectionType(connectionType)
                    .setConnectedEntityType(entityType.getNumber())
                    .setConnectedEntityId(id).build();
        }
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

    private static ActionDTO.Action executableContainerAtomicResize(long workloadControllerId,
            long containerSpecId, Collection<Long> containerIds, Severity severity) {
        return ActionDTO.Action.newBuilder()
                .setExecutable(true)
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(mapSeverityToImportance(severity))
                .setInfo(TestActionBuilder.makeAtomicResizeInfo(workloadControllerId, containerSpecId, containerIds))
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
