package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;

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

    private final SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());
    private final RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(
        supplyChainServiceMole,
        repositoryServiceMole);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        entitySeverityCache = new EntitySeverityCache(
            SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    @Test
    public void testRefreshVisibleNotDisabled() {
        ActionView action = actionView(executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.CRITICAL));
        when(actionStore.getActionViews())
            .thenReturn(new MapBackedActionViews(Collections.singletonMap(action.getRecommendation().getId(), action)));

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testRefreshNotVisibleDisabled() {
        ActionView action = spy(actionView(executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.CRITICAL)));
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
        ActionView action1 = actionView(executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.MINOR));
        ActionView action2 = actionView(executableMove(3, DEFAULT_SOURCE_ID, 1, 4, 1, Severity.CRITICAL));
        ActionView action3 = actionView(executableMove(5, DEFAULT_SOURCE_ID, 1, 6, 1, Severity.MAJOR));

        when(actionStore.getActionViews()).thenReturn(new MapBackedActionViews(ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3)));

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testRefreshIndividualAction() {
        ActionView action1 = actionView(executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.MINOR));
        ActionView action2 = spy(actionView(executableMove(3, DEFAULT_SOURCE_ID, 1, 4, 1, Severity.CRITICAL)));
        ActionView action3 = actionView(executableMove(5, DEFAULT_SOURCE_ID, 1, 6, 1, Severity.MAJOR));
        ActionView unrelatedAction = actionView(executableMove(5, 999, 1, 6, 1, Severity.CRITICAL));

        final QueryableActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3,
            unrelatedAction.getRecommendation().getId(), unrelatedAction));
        when(actionStore.getActionViews()).thenReturn(actionViews);

        when(actionStore.getActionView(Mockito.anyLong()))
            .thenAnswer((Answer<Optional<ActionView>>) invocation -> {
                Long id = invocation.getArgumentAt(0, Long.class);
                return actionViews.get(id);
            });

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());

        // Simulate changing the state of the critical action to IN_PROGRESS and refreshing for that action.
        // State should the next most critical action that applies to the entity.
        when(action2.getState()).thenReturn(ActionState.IN_PROGRESS);

        entitySeverityCache.refresh(action2.getRecommendation(), actionStore);
        assertEquals(Severity.MAJOR, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    /**
     * VMs, Hosts, etc should only count actions where they are directly the source.
     * BusinessApp should count actions in entities below them in the supply chain.
     */
    @Test
    public void testSeverityCounts() {
        final long virtualMachine1Oid = 111L;
        final long physicalMachine1Oid = 222L;
        final long storage1Oid = 333L;
        final long missingId = 444L;
        long idCounter = 1000;
        final long applicationOid = idCounter++;
        final long businessApp1Oid = idCounter++;
        final long virtualMachine2Oid = idCounter++;
        final long physicalMachine2Oid = idCounter++;
        final long storage2Oid = idCounter++;
        final long databaseOid = idCounter++;
        final long businessApp2Oid = idCounter++;
        final long service1Oid = idCounter++;
        final long service2Oid = idCounter++;
        final long businessTx1Oid = idCounter++;
        final long businessTx2Oid = idCounter++;

        List<ActionView> actions = Arrays.asList(
            actionView(executableMove(0, virtualMachine1Oid, 1, 2, 1, Severity.MINOR)),
            actionView(executableMove(3, physicalMachine1Oid, 1, 4, 1, Severity.MINOR)),
            actionView(executableMove(5, storage1Oid, 1, 6, 1, Severity.MAJOR)),
            actionView(executableMove(5, databaseOid, 1, 2, 1, Severity.CRITICAL)),
            actionView(executableMove(5, storage2Oid, 1, 2, 1, Severity.MINOR)),
            actionView(executableMove(5, storage2Oid, 1, 2, 1, Severity.MAJOR))
        );

        // refresh cache using the above actions
        when(actionStore.getActionViews()).thenReturn(new MapBackedActionViews(
            actions.stream().collect(Collectors.toMap(actionView -> actionView.getRecommendation().getId(), Function.identity()))));

        // repository service will return the two business apps
        when(repositoryServiceMole.retrieveTopologyEntities(any())).thenReturn(
            Arrays.asList(PartialEntityBatch.newBuilder()
                .addAllEntities(Stream.of(businessApp1Oid, businessApp2Oid, businessTx1Oid, businessTx2Oid, service1Oid, service2Oid)
                    .map(businessAppOid -> PartialEntity.newBuilder()
                        .setMinimal(MinimalEntity.newBuilder()
                            .setOid(businessAppOid)
                            .buildPartial())
                        .buildPartial())
                    .collect(Collectors.toList()))
                .buildPartial()));

        // listen to the supply chain request that is sent.
        // later on we verify they it contains a request for bapp1 and another request for bapp2
        // batched together
        ArgumentCaptor<GetMultiSupplyChainsRequest> getMultiSupplyChainsCaptor = ArgumentCaptor.forClass(GetMultiSupplyChainsRequest.class);
        when(supplyChainServiceMole.getMultiSupplyChains(getMultiSupplyChainsCaptor.capture())).thenReturn(
            Arrays.asList(
                makeGetMultiSupplyChainsResponse(businessApp1Oid, businessApp1Oid, businessTx1Oid, service1Oid, applicationOid, virtualMachine1Oid, physicalMachine1Oid, storage1Oid),
                makeGetMultiSupplyChainsResponse(businessApp2Oid, businessApp2Oid, businessTx2Oid, service2Oid, databaseOid, virtualMachine2Oid, physicalMachine2Oid, storage2Oid),
                makeGetMultiSupplyChainsResponse(businessTx1Oid, businessTx1Oid, service1Oid, applicationOid, virtualMachine1Oid, physicalMachine1Oid, storage1Oid),
                makeGetMultiSupplyChainsResponse(businessTx2Oid, businessTx2Oid, service2Oid, databaseOid, virtualMachine2Oid, physicalMachine2Oid, storage2Oid),
                makeGetMultiSupplyChainsResponse(service1Oid, service1Oid, applicationOid, virtualMachine1Oid, physicalMachine1Oid, storage1Oid),
                makeGetMultiSupplyChainsResponse(service2Oid, service2Oid, databaseOid, virtualMachine2Oid, physicalMachine2Oid, storage2Oid)
            ));

        entitySeverityCache.refresh(actionStore);
        Map<Optional<Severity>, Long> actualSeverityCounts =
            entitySeverityCache.getSeverityCounts(Arrays.asList(virtualMachine1Oid, physicalMachine1Oid, storage1Oid, missingId));
        assertEquals(2L, (long)actualSeverityCounts.get(Optional.of(Severity.MINOR)));
        assertEquals(1L, (long)actualSeverityCounts.get(Optional.of(Severity.MAJOR)));
        // No actions should map to critical so it should be empty.
        assertTrue(actualSeverityCounts.get(Optional.of(Severity.CRITICAL)) == null);
        assertEquals(1L, (long)actualSeverityCounts.get(Optional.<Severity>empty())); // The missing ID should map to null

        // check that the appropriate request was sent to supply chain service
        GetMultiSupplyChainsRequest actualSupplyChainRequest = getMultiSupplyChainsCaptor.getValue();
        assertEquals(6, actualSupplyChainRequest.getSeedsCount());
        assertEquals(
            ImmutableSet.of(businessApp1Oid, businessApp2Oid, businessTx1Oid, businessTx2Oid, service1Oid, service2Oid),
            actualSupplyChainRequest.getSeedsList().stream()
                .map(SupplyChainSeed::getSeedOid)
                .collect(Collectors.toSet()));
        assertEquals(
            ImmutableSet.of(businessApp1Oid, businessApp2Oid, businessTx1Oid, businessTx2Oid, service1Oid, service2Oid),
            actualSupplyChainRequest.getSeedsList().stream()
                .map(supplyChainSeed -> supplyChainSeed.getScope().getStartingEntityOid(0))
                .collect(Collectors.toSet()));

        // BusinessApp1 -> BTx -> Srv -> App -> VM1 -> PM1 -> Storage1
        //                               Minor  Minor    Major
        // Minor: 2
        // Major: 1
        // Normal: 2
        // BusinessApp1, BusinessTx1, Service1 should have the same counts except NORMAL
        // BusinessApp1
        actualSeverityCounts =
            entitySeverityCache.getSeverityCounts(Arrays.asList(businessApp1Oid));
        assertEquals(ImmutableMap.of(
                Optional.of(Severity.MINOR), 2L,
                Optional.of(Severity.MAJOR), 1L,
                Optional.of(Severity.NORMAL), 4L),
            actualSeverityCounts);
        // BusinessTx1
        actualSeverityCounts =
                entitySeverityCache.getSeverityCounts(Arrays.asList(businessTx1Oid));
        assertEquals(ImmutableMap.of(
                Optional.of(Severity.MINOR), 2L,
                Optional.of(Severity.MAJOR), 1L,
                Optional.of(Severity.NORMAL), 3L),
                actualSeverityCounts);
        // Service1
        actualSeverityCounts =
                entitySeverityCache.getSeverityCounts(Arrays.asList(service1Oid));
        assertEquals(ImmutableMap.of(
                Optional.of(Severity.MINOR), 2L,
                Optional.of(Severity.MAJOR), 1L,
                Optional.of(Severity.NORMAL), 2L),
                actualSeverityCounts);


        // BusinessApp2 -> BTx -> Srv -> DB -> VM2 -> PM2 -> Storage2
        //                            Critical               Major
        //                                                   Minor
        // Critical: 1
        // Major: 1
        // Normal: 3
        // no minor because Storage2's max severity is major
        // BusinessApp2, BusinessTx2, Service3 should have the same counts except NORMAL
        // BusinessApp2
        actualSeverityCounts =
            entitySeverityCache.getSeverityCounts(Arrays.asList(businessApp2Oid));
        assertEquals(ImmutableMap.of(
            Optional.of(Severity.CRITICAL), 1L,
            Optional.of(Severity.MAJOR), 1L,
            Optional.of(Severity.NORMAL), 5L),
            actualSeverityCounts);
        // BusinessTx2
        actualSeverityCounts =
            entitySeverityCache.getSeverityCounts(Arrays.asList(businessTx2Oid));
        assertEquals(ImmutableMap.of(
            Optional.of(Severity.CRITICAL), 1L,
            Optional.of(Severity.MAJOR), 1L,
            Optional.of(Severity.NORMAL), 4L),
            actualSeverityCounts);
        // Service2
        actualSeverityCounts =
            entitySeverityCache.getSeverityCounts(Arrays.asList(service2Oid));
        assertEquals(ImmutableMap.of(
            Optional.of(Severity.CRITICAL), 1L,
            Optional.of(Severity.MAJOR), 1L,
            Optional.of(Severity.NORMAL), 3L),
            actualSeverityCounts);
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
            ImmutableMap.of(Severity.MINOR, 2L,
                Severity.NORMAL, 2L)
        )));
        // exact same severity as previous should be tied
        long exactSameSeverity = 99L;
        when(mockedEntitySeverityCache.getSeverity(exactSameSeverity)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(exactSameSeverity)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 2L,
                Severity.NORMAL, 2L)
        )));
        // same proportion but higher count
        long sameProportionHigherCount = id++;
        when(mockedEntitySeverityCache.getSeverity(sameProportionHigherCount)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(sameProportionHigherCount)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 4L,
                Severity.NORMAL, 4L)
        )));
        // higher proportion
        long higherProportion = id++;
        when(mockedEntitySeverityCache.getSeverity(higherProportion)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(higherProportion)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 3L,
                Severity.NORMAL, 2L)
        )));
        // higher severity
        long higherSeverity = id++;
        when(mockedEntitySeverityCache.getSeverity(higherSeverity)).thenReturn(Optional.of(Severity.MAJOR));
        when(mockedEntitySeverityCache.getSeverityBreakdown(higherSeverity)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 2L,
                Severity.NORMAL, 2L,
                Severity.MAJOR, 2L)
        )));
        // even higher severity, but no entity level severity
        long evenHigherSeverityButNoEntitySeverity = id++;
        when(mockedEntitySeverityCache.getSeverity(evenHigherSeverityButNoEntitySeverity)).thenReturn(Optional.empty());
        when(mockedEntitySeverityCache.getSeverityBreakdown(evenHigherSeverityButNoEntitySeverity)).thenReturn(Optional.of(makeSeverityCount(
            ImmutableMap.of(Severity.MINOR, 2L,
                Severity.NORMAL, 2L,
                Severity.CRITICAL, 2L)
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

    private SeverityCount makeSeverityCount(Map<Severity, Long> severityBreakdown) {
        SeverityCount severityCount = new SeverityCount();
        for (Map.Entry<Severity, Long> entry : severityBreakdown.entrySet()) {
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

    private static GetMultiSupplyChainsResponse makeGetMultiSupplyChainsResponse(long seedOid, long...oids) {
        return GetMultiSupplyChainsResponse.newBuilder()
            .setSeedOid(seedOid)
            .setSupplyChain(SupplyChain.newBuilder()
                .addAllSupplyChainNodes(Arrays.stream(oids).boxed()
                    .map(oid -> SupplyChainNode.newBuilder()
                        .putMembersByState(0, MemberList.newBuilder().addMemberOids(oid).buildPartial())
                        .buildPartial())
                    .collect(Collectors.toList()))
                .buildPartial())
            .buildPartial();
    }

    @Nonnull
    private ActionView actionView(ActionDTO.Action recommendation) {
        return actionFactory.newAction(recommendation, ACTION_PLAN_ID);
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
