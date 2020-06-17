package com.vmturbo.action.orchestrator.stats.query.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander.InvolvedEntitiesFilter;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.EntityScope;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.GlobalScope;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest.SingleQuery;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class QueryInfoFactoryTest {
    private static final long REALTIME_CONTEXT_ID = 77777;

    private static final int VM = EntityType.VIRTUAL_MACHINE_VALUE;

    private static final ActionEntity CLOUD_VM = ActionEntity.newBuilder()
        .setId(7)
        .setType(VM)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();

    private static final ActionEntity ON_PREM_VM = ActionEntity.newBuilder()
        .setId(8)
        .setType(VM)
        .setEnvironmentType(EnvironmentType.ON_PREM)
        .build();

    private static final ActionEntity ON_PREM_PM = ActionEntity.newBuilder()
            .setId(123)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .build();


    private UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);

    private InvolvedEntitiesExpander involvedEntitiesExpander = Mockito.mock(InvolvedEntitiesExpander.class);

    private QueryInfoFactory queryInfoFactory = new QueryInfoFactory(
        REALTIME_CONTEXT_ID,
        userSessionContext,
        involvedEntitiesExpander);

    /**
     * Setup.
     */
    @Before
    public void setup() {
        // default user will be unscoped.
        when(userSessionContext.isUserScoped()).thenReturn(false);
        when(involvedEntitiesExpander.expandInvolvedEntitiesFilter(anyCollection())).thenAnswer(
            (Answer<InvolvedEntitiesFilter>)invocationOnMock -> {
                Set<Long> oids = new HashSet<>(
                    (Collection<Long>)(invocationOnMock.getArguments()[0]));
                return new InvolvedEntitiesFilter(
                    oids, InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES);
            }
        );
    }

    @Test
    public void testQueryInfoExtractionSpecificTopologyContext() {
        final CurrentActionStatsQuery liveQuery = CurrentActionStatsQuery.newBuilder()
            .setScopeFilter(ScopeFilter.newBuilder()
                .setTopologyContextId(123)
                .setEntityList(EntityScope.newBuilder()
                    .addOids(7)))
            .build();

        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQueryId(4)
            .setQuery(liveQuery)
            .build());

        assertThat(queryInfo.query(), is(liveQuery));
        assertThat(queryInfo.queryId(), is(4L));
        assertThat(queryInfo.topologyContextId(), is(123L));
    }

    @Test
    public void testQueryInfoExtractionDefaultTopologyContext() {
        final CurrentActionStatsQuery liveQuery = CurrentActionStatsQuery.newBuilder()
            .build();

        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQueryId(4)
            .setQuery(liveQuery)
            .build());

        assertThat(queryInfo.query(), is(liveQuery));
        assertThat(queryInfo.queryId(), is(4L));
        assertThat(queryInfo.topologyContextId(), is(REALTIME_CONTEXT_ID));
    }

    @Test
    public void testQueryEntityPredicateEntityList() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setScopeFilter(ScopeFilter.newBuilder()
                    .setEntityList(EntityScope.newBuilder()
                        .addOids(CLOUD_VM.getId()))))
            .build());

        assertTrue(queryInfo.entityPredicate().test(CLOUD_VM));
        assertFalse(queryInfo.entityPredicate().test(ON_PREM_VM));
    }

    @Test
    public void testQueryMarketEnvironmentTypeCloud() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setScopeFilter(ScopeFilter.newBuilder()
                    .setGlobal(GlobalScope.newBuilder()
                        .setEnvironmentType(EnvironmentType.CLOUD))))
            .build());

        assertTrue(queryInfo.entityPredicate().test(CLOUD_VM));
        assertFalse(queryInfo.entityPredicate().test(ON_PREM_VM));
    }

    @Test
    public void testQueryMarketEnvironmentTypeOnPrem() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setScopeFilter(ScopeFilter.newBuilder()
                    .setGlobal(GlobalScope.newBuilder()
                        .setEnvironmentType(EnvironmentType.ON_PREM))))
            .build());

        final ActionEntity unsetEnvVm = ActionEntity.newBuilder()
            .setId(123)
            .setType(VM)
            .build();
        assertTrue(queryInfo.entityPredicate().test(ON_PREM_VM));
        assertFalse(queryInfo.entityPredicate().test(CLOUD_VM));
        assertFalse(queryInfo.entityPredicate().test(unsetEnvVm));
    }

    @Test
    public void testQueryMarketMatchAll() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setScopeFilter(ScopeFilter.newBuilder()
                    // Want the whole market. All entities should match.
                    .setGlobal(GlobalScope.getDefaultInstance())))
            .build());

        assertTrue(queryInfo.entityPredicate().test(CLOUD_VM));
        assertTrue(queryInfo.entityPredicate().test(ON_PREM_VM));
    }

    @Test
    public void testQueryMarketEntityType() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setScopeFilter(ScopeFilter.newBuilder()
                    .setGlobal(GlobalScope.newBuilder()
                        .addEntityType(VM))))
            .build());
        assertTrue(queryInfo.entityPredicate().test(CLOUD_VM));
        assertTrue(queryInfo.entityPredicate().test(ON_PREM_VM));
        assertFalse(queryInfo.entityPredicate().test(ON_PREM_PM));
    }

    @Test
    public void testQueryMarketGlobalScopeARMEntityType() {

        long belowARMEntityId = 1L;
        long notBelowARMEntityId = 2L;

        when(involvedEntitiesExpander.isARMEntityType(EntityType.SERVICE_VALUE)).thenReturn(true);
        when(involvedEntitiesExpander.isBelowARMEntityType(belowARMEntityId,
                Collections.singleton(EntityType.SERVICE_VALUE))).thenReturn(true);
        when(involvedEntitiesExpander.isBelowARMEntityType(notBelowARMEntityId,
                Collections.singleton(EntityType.SERVICE_VALUE))).thenReturn(false);

        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
                .setQuery(CurrentActionStatsQuery.newBuilder()
                        .setScopeFilter(ScopeFilter.newBuilder()
                                .setGlobal(GlobalScope.newBuilder()
                                        .addEntityType(EntityType.SERVICE_VALUE))))
                .build());

        assertTrue(queryInfo.entityPredicate().test(ActionEntity.newBuilder()
                .setId(belowARMEntityId)
                .setType(VM)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build()));

        assertFalse(queryInfo.entityPredicate().test(ActionEntity.newBuilder()
                .setId(notBelowARMEntityId)
                .setType(EntityType.RESERVED_INSTANCE_VALUE)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build()));
    }

    @Test
    public void testQueryMarketWithScopedUser() {
        // create a scoped user that only has access to the on-prem VM and on-prem PM
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(ON_PREM_VM.getId(), ON_PREM_PM.getId())), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        // Now, a general market query should only see the on prem vm.
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
                .setQuery(CurrentActionStatsQuery.newBuilder()
                        .setScopeFilter(ScopeFilter.newBuilder()
                                .setGlobal(GlobalScope.getDefaultInstance())))
                .build());
        assertTrue(queryInfo.entityPredicate().test(ON_PREM_VM));
        assertFalse(queryInfo.entityPredicate().test(CLOUD_VM));
        assertTrue(queryInfo.entityPredicate().test(ON_PREM_PM));
    }

    @Test
    public void testQueryMarketByEntityTypeWithScopedUser() {
        // create a scoped user that only has access to the on-prem VM and PM
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(ON_PREM_VM.getId(), ON_PREM_PM.getId())), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        // Now, a market query for vm entities should only see the on prem vm.
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
                .setQuery(CurrentActionStatsQuery.newBuilder()
                        .setScopeFilter(ScopeFilter.newBuilder()
                                .setGlobal(GlobalScope.newBuilder()
                                        .addEntityType(VM))))
                .build());
        assertTrue(queryInfo.entityPredicate().test(ON_PREM_VM));
        assertFalse(queryInfo.entityPredicate().test(CLOUD_VM));
        assertFalse(queryInfo.entityPredicate().test(ON_PREM_PM));
    }

    @Test
    public void testQueryMarketByEntityTypeAndEnvironmentWithScopedUser() {
        // create a scoped user that only has access to the on-prem VM and cloud VM
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(ON_PREM_VM.getId(), CLOUD_VM.getId())), null);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        // Now, a market query for on-prem VM entities should only see the on prem vm.
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
                .setQuery(CurrentActionStatsQuery.newBuilder()
                        .setScopeFilter(ScopeFilter.newBuilder()
                                .setGlobal(GlobalScope.newBuilder()
                                        .setEnvironmentType(EnvironmentType.ON_PREM)
                                        .addEntityType(VM))))
                .build());
        assertTrue(queryInfo.entityPredicate().test(ON_PREM_VM));
        assertFalse(queryInfo.entityPredicate().test(CLOUD_VM));
        assertFalse(queryInfo.entityPredicate().test(ON_PREM_PM));
    }

    @Test
    public void testQueryActionTypeFilter() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setActionGroupFilter(ActionGroupFilter.newBuilder()
                    .addActionType(ActionType.ACTIVATE)))
            .build());

        final SingleActionInfo matching = activateActionInfo(
            actionBuilder -> actionBuilder.setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(CLOUD_VM))),
            actionView -> {});
        final SingleActionInfo nonMatching = activateActionInfo(
            actionBuilder -> actionBuilder.setInfo(ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder()
                    .setTarget(CLOUD_VM))),
            actionView -> {});
        assertTrue(queryInfo.actionGroupPredicate().test(matching));
        assertFalse(queryInfo.actionGroupPredicate().test(nonMatching));
    }

    @Test
    public void testQueryActionCategoryFilter() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setActionGroupFilter(ActionGroupFilter.newBuilder()
                    .addActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)))
            .build());

        final SingleActionInfo matching = activateActionInfo(
            actionBuilder -> {},
            actionView -> {
                when(actionView.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
            });
        final SingleActionInfo nonMatching = activateActionInfo(
            actionBuilder -> {},
            actionView -> {
                when(actionView.getActionCategory()).thenReturn(ActionCategory.COMPLIANCE);
            });
        assertTrue(queryInfo.actionGroupPredicate().test(matching));
        assertFalse(queryInfo.actionGroupPredicate().test(nonMatching));
    }

    @Test
    public void testQueryActionStateFilter() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setActionGroupFilter(ActionGroupFilter.newBuilder()
                    .addActionState(ActionState.IN_PROGRESS)))
            .build());

        final SingleActionInfo matching = activateActionInfo(
            actionBuilder -> {},
            actionView -> {
                when(actionView.getState()).thenReturn(ActionState.IN_PROGRESS);
            });
        final SingleActionInfo nonMatching = activateActionInfo(
            actionBuilder -> {},
            actionView -> {
                when(actionView.getState()).thenReturn(ActionState.SUCCEEDED);
            });
        assertTrue(queryInfo.actionGroupPredicate().test(matching));
        assertFalse(queryInfo.actionGroupPredicate().test(nonMatching));
    }

    @Test
    public void testQueryActionModeFilter() {
        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setActionGroupFilter(ActionGroupFilter.newBuilder()
                    .addActionMode(ActionMode.RECOMMEND)))
            .build());

        final SingleActionInfo matching = activateActionInfo(
            actionBuilder -> {},
            actionView -> {
                when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);
            });
        final SingleActionInfo nonMatching = activateActionInfo(
            actionBuilder -> {},
            actionView -> {
                when(actionView.getMode()).thenReturn(ActionMode.AUTOMATIC);
            });
        assertTrue(queryInfo.actionGroupPredicate().test(matching));
        assertFalse(queryInfo.actionGroupPredicate().test(nonMatching));
    }

    /**
     * QueryInfoFactory should populate the desired entities and the predicate with the expanded
     * entities when using all ARM entities.
     */
    @Test
    public void testExpandedFilter() {
        Set<Long> expandedSet = ImmutableSet.of(1L, 2L, 3L);
        when(involvedEntitiesExpander.expandInvolvedEntitiesFilter(anyCollection()))
            .thenReturn(new InvolvedEntitiesFilter(
                expandedSet,
                InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS));

        final QueryInfo queryInfo = queryInfoFactory.extractQueryInfo(SingleQuery.newBuilder()
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setScopeFilter(ScopeFilter.newBuilder()
                    .setTopologyContextId(123)
                    .setEntityList(EntityScope.newBuilder()
                        .addOids(1L)))
                .build())
            .build());

        Assert.assertTrue(queryInfo.desiredEntities() != null);
        Assert.assertEquals(expandedSet, queryInfo.desiredEntities());
        Assert.assertTrue(queryInfo.entityPredicate().test(actionEntity(1L)));
        Assert.assertTrue(queryInfo.entityPredicate().test(actionEntity(2L)));
        Assert.assertTrue(queryInfo.entityPredicate().test(actionEntity(3L)));
        Assert.assertFalse(queryInfo.entityPredicate().test(actionEntity(4L)));
    }

    @Nonnull
    private SingleActionInfo activateActionInfo(@Nonnull final Consumer<Action.Builder> actionCustomizer,
                                                @Nonnull final Consumer<ActionView> actionViewConsumer) {
        final ActionDTO.Action.Builder builder = Action.newBuilder()
            .setId(1)
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(CLOUD_VM)))
            .setExplanation(Explanation.getDefaultInstance())
            .setDeprecatedImportance(1);
        actionCustomizer.accept(builder);

        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(builder.build());
        actionViewConsumer.accept(actionView);

        final SingleActionInfo singleActionInfo = ImmutableSingleActionInfo.builder()
            .action(actionView)
            .build();
        return singleActionInfo;
    }

    @Nonnull
    private ActionEntity actionEntity(long oid) {
        return ActionEntity.newBuilder()
            .setId(oid)
            .buildPartial();
    }
}
