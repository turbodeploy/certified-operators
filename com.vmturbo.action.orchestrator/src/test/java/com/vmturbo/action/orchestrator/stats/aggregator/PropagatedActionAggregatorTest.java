package com.vmturbo.action.orchestrator.stats.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.ImmutableStatsActionView;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Unit tests for {@link PropagatedActionAggregator}.
 */
public class PropagatedActionAggregatorTest {

    private static final LocalDateTime TIME = LocalDateTime.MAX;

    private static final long INVOLVED_ENTITY_ID = 1234L;
    private static final long UNINVOLVED_ENTITY_ID = 4321L;
    private static final long INVOLVED_PM_ENTITY_ID = 12345L;

    private static final ActionGroup.ActionGroupKey
                    ACTION_GROUP_KEY = mock(ActionGroup.ActionGroupKey.class);
    private static final ActionGroup ACTION_GROUP = ImmutableActionGroup.builder()
                    .id(888)
                    .key(ACTION_GROUP_KEY)
                    .build();

    private PropagatedActionAggregator.PropagatedActionAggregatorFactory aggregatorFactory;

    private ActionDTO.Action action;

    private MgmtUnitSubgroup subgroupService;
    private MgmtUnitSubgroup subgroupBT;
    private MgmtUnitSubgroup subgroupBA;
    private MgmtUnitSubgroup subgroupNamespace;
    private MgmtUnitSubgroup subgroupContainer;

    /**
     * Test preparing.
     */
    @Before
    public void setUp() {
        InvolvedEntitiesExpander expander = Mockito.mock(InvolvedEntitiesExpander.class);
        Mockito.when(expander.shouldPropagateAction(INVOLVED_ENTITY_ID, Collections.singleton(ApiEntityType.SERVICE.typeNumber())))
                        .thenReturn(true);
        Mockito.when(expander.shouldPropagateAction(INVOLVED_ENTITY_ID, Collections.singleton(ApiEntityType.BUSINESS_APPLICATION.typeNumber())))
                        .thenReturn(true);
        Mockito.when(expander.shouldPropagateAction(INVOLVED_ENTITY_ID, Collections.singleton(ApiEntityType.BUSINESS_TRANSACTION.typeNumber())))
                        .thenReturn(true);
        Mockito.when(expander.shouldPropagateAction(INVOLVED_ENTITY_ID, Collections.singleton(ApiEntityType.NAMESPACE.typeNumber())))
                        .thenReturn(true);
        Mockito.when(expander.shouldPropagateAction(INVOLVED_ENTITY_ID, Collections.singleton(ApiEntityType.CONTAINER_PLATFORM_CLUSTER.typeNumber())))
                        .thenReturn(true);

        Mockito.when(expander.shouldPropagateAction(UNINVOLVED_ENTITY_ID, Collections.singleton(ApiEntityType.SERVICE.typeNumber())))
                        .thenReturn(false);
        Mockito.when(expander.shouldPropagateAction(UNINVOLVED_ENTITY_ID, Collections.singleton(ApiEntityType.BUSINESS_APPLICATION.typeNumber())))
                        .thenReturn(false);
        Mockito.when(expander.shouldPropagateAction(UNINVOLVED_ENTITY_ID, Collections.singleton(ApiEntityType.BUSINESS_TRANSACTION.typeNumber())))
                        .thenReturn(false);

        Mockito.when(expander.shouldPropagateAction(INVOLVED_PM_ENTITY_ID, Collections.singleton(ApiEntityType.SERVICE.typeNumber())))
                        .thenReturn(true);
        Mockito.when(expander.shouldPropagateAction(INVOLVED_PM_ENTITY_ID, Collections.singleton(ApiEntityType.BUSINESS_APPLICATION.typeNumber())))
                        .thenReturn(true);

        Mockito.when(expander.shouldPropagateAction(INVOLVED_PM_ENTITY_ID, Collections.singleton(ApiEntityType.BUSINESS_TRANSACTION.typeNumber())))
                        .thenReturn(false);

        aggregatorFactory = new PropagatedActionAggregator.PropagatedActionAggregatorFactory(expander);

        action = ActionDTO.Action.newBuilder()
                        .setId(1)
                        .setInfo(ActionDTO.ActionInfo.newBuilder()
                                        .setActivate(ActionDTO.Activate.newBuilder()
                                                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                                                                        .setId(1)
                                                                        .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                                                                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM))))
                        .setExplanation(ActionDTO.Explanation.getDefaultInstance())
                        .setDeprecatedImportance(1)
                        .build();

        subgroupService = ImmutableMgmtUnitSubgroup.builder()
                        .id(1).key(ImmutableMgmtUnitSubgroupKey.builder()
                                   .mgmtUnitId(0)
                                   .mgmtUnitType(ManagementUnitType.PROPAGATED)
                                   .environmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                                   .entityType(CommonDTO.EntityDTO.EntityType.SERVICE_VALUE)
                                   .build()).build();

        subgroupBT = ImmutableMgmtUnitSubgroup.builder()
                        .id(2).key(ImmutableMgmtUnitSubgroupKey.builder()
                                   .mgmtUnitId(0)
                                   .mgmtUnitType(ManagementUnitType.PROPAGATED)
                                   .environmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                                   .entityType(CommonDTO.EntityDTO.EntityType.BUSINESS_TRANSACTION_VALUE)
                                   .build()).build();

        subgroupBA = ImmutableMgmtUnitSubgroup.builder()
                        .id(3).key(ImmutableMgmtUnitSubgroupKey.builder()
                                   .mgmtUnitId(0)
                                   .mgmtUnitType(ManagementUnitType.PROPAGATED)
                                   .environmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                                   .entityType(CommonDTO.EntityDTO.EntityType.BUSINESS_APPLICATION_VALUE)
                                   .build()).build();

        subgroupNamespace = ImmutableMgmtUnitSubgroup.builder()
                        .id(4).key(ImmutableMgmtUnitSubgroupKey.builder()
                                   .mgmtUnitId(0)
                                   .mgmtUnitType(ManagementUnitType.PROPAGATED)
                                   .environmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                                   .entityType(CommonDTO.EntityDTO.EntityType.NAMESPACE_VALUE)
                                   .build()).build();

        subgroupContainer = ImmutableMgmtUnitSubgroup.builder()
                        .id(5).key(ImmutableMgmtUnitSubgroupKey.builder()
                                   .mgmtUnitId(0)
                                   .mgmtUnitType(ManagementUnitType.PROPAGATED)
                                   .environmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                                   .entityType(CommonDTO.EntityDTO.EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
                                   .build()).build();
    }

    /**
     * Check that action stats records are created for Service, Business Transaction
     * and Business Application if the VM, which are involved into the action, involves these entities.
     */
    @Test
    public void testInvolvedAggregation() {
        final ActionDTO.ActionEntity vm = ActionDTO.ActionEntity.newBuilder()
                        .setType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .setId(INVOLVED_ENTITY_ID)
                        .build();

        final ImmutableStatsActionView actionSnapshot = ImmutableStatsActionView.builder()
                        .actionGroupKey(ACTION_GROUP_KEY)
                        .recommendation(action)
                        .primaryEntity(vm)
                        .build();

        final PropagatedActionAggregator aggregator = aggregatorFactory.newAggregator(TIME);
        aggregator.processAction(actionSnapshot, new LiveActionsStatistician.PreviousBroadcastActions());

        final Map<Integer, ActionStatsLatestRecord> records =
                        aggregator.createRecords(ImmutableMap.of(subgroupService.key(), subgroupService,
                                                                 subgroupBT.key(), subgroupBT,
                                                                 subgroupBA.key(), subgroupBA,
                                                                 subgroupNamespace.key(), subgroupNamespace,
                                                                 subgroupContainer.key(), subgroupContainer),
                                                 ImmutableMap.of(ACTION_GROUP_KEY, ACTION_GROUP))
                                        .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        final ActionStatsLatestRecord serviceRecord = records.get(subgroupService.id());
        assertThat(serviceRecord.getTotalActionCount(), is(1));
        assertThat(serviceRecord.getTotalEntityCount(), is(1));
        assertThat(serviceRecord.getMgmtUnitSubgroupId(), is(subgroupService.id()));
        assertThat(serviceRecord.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(serviceRecord.getTotalSavings().doubleValue(), closeTo(action.getSavingsPerHour().getAmount(), 0.001));
        assertThat(serviceRecord.getTotalInvestment().doubleValue(), is(0.0));

        final ActionStatsLatestRecord btRecord = records.get(subgroupBT.id());
        assertThat(btRecord.getTotalActionCount(), is(1));
        assertThat(btRecord.getTotalEntityCount(), is(1));
        assertThat(btRecord.getMgmtUnitSubgroupId(), is(subgroupBT.id()));
        assertThat(btRecord.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(btRecord.getTotalSavings().doubleValue(), closeTo(action.getSavingsPerHour().getAmount(), 0.001));
        assertThat(btRecord.getTotalInvestment().doubleValue(), is(0.0));

        final ActionStatsLatestRecord baRecord = records.get(subgroupBA.id());
        assertThat(baRecord.getTotalActionCount(), is(1));
        assertThat(baRecord.getTotalEntityCount(), is(1));
        assertThat(baRecord.getMgmtUnitSubgroupId(), is(subgroupBA.id()));
        assertThat(baRecord.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(baRecord.getTotalSavings().doubleValue(), closeTo(action.getSavingsPerHour().getAmount(), 0.001));
        assertThat(baRecord.getTotalInvestment().doubleValue(), is(0.0));

        final ActionStatsLatestRecord namespaceRecord = records.get(subgroupNamespace.id());
        assertThat(namespaceRecord.getTotalActionCount(), is(1));
        assertThat(namespaceRecord.getTotalEntityCount(), is(1));
        assertThat(namespaceRecord.getMgmtUnitSubgroupId(), is(subgroupNamespace.id()));
        assertThat(namespaceRecord.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(namespaceRecord.getTotalSavings().doubleValue(), closeTo(action.getSavingsPerHour().getAmount(), 0.001));
        assertThat(namespaceRecord.getTotalInvestment().doubleValue(), is(0.0));

        final ActionStatsLatestRecord container = records.get(subgroupContainer.id());
        assertThat(container.getTotalActionCount(), is(1));
        assertThat(container.getTotalEntityCount(), is(1));
        assertThat(container.getMgmtUnitSubgroupId(), is(subgroupContainer.id()));
        assertThat(container.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(container.getTotalSavings().doubleValue(), closeTo(action.getSavingsPerHour().getAmount(), 0.001));
        assertThat(container.getTotalInvestment().doubleValue(), is(0.0));
    }

    /**
     * Check that action stats records won't be created if no action entities don't involve
     * propagated entities.
     */
    @Test
    public void testUninvolvedAggregation() {
        final ActionDTO.ActionEntity vm = ActionDTO.ActionEntity.newBuilder()
                        .setType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .setId(UNINVOLVED_ENTITY_ID)
                        .build();

        final ImmutableStatsActionView actionSnapshot = ImmutableStatsActionView.builder()
                        .actionGroupKey(ACTION_GROUP_KEY)
                        .recommendation(action)
                        .primaryEntity(vm)
                        .build();

        final PropagatedActionAggregator aggregator = aggregatorFactory.newAggregator(TIME);
        aggregator.processAction(actionSnapshot, new LiveActionsStatistician.PreviousBroadcastActions());

        final Map<Integer, ActionStatsLatestRecord> records =
                        aggregator.createRecords(ImmutableMap.of(subgroupService.key(), subgroupService,
                                                                 subgroupBT.key(), subgroupBT,
                                                                 subgroupBA.key(), subgroupBA),
                                                 ImmutableMap.of(ACTION_GROUP_KEY, ACTION_GROUP))
                                        .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        Assert.assertEquals(0, records.size());
    }

    /**
     * Check that only one action stats record for each propagated entity if the action has several
     * involved entities.
     */
    @Test
    public void testSeveralInvolvedAggregation() {
        final ActionDTO.ActionEntity vm = ActionDTO.ActionEntity.newBuilder()
                        .setType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .setId(INVOLVED_ENTITY_ID)
                        .build();

        final ActionDTO.ActionEntity host = ActionDTO.ActionEntity.newBuilder()
                        .setType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .setId(INVOLVED_PM_ENTITY_ID)
                        .build();

        final ImmutableStatsActionView actionSnapshot = ImmutableStatsActionView.builder()
                        .actionGroupKey(ACTION_GROUP_KEY)
                        .recommendation(action)
                        .primaryEntity(vm)
                        .build();

        final PropagatedActionAggregator aggregator = aggregatorFactory.newAggregator(TIME);
        aggregator.processAction(actionSnapshot, new LiveActionsStatistician.PreviousBroadcastActions());

        final Map<Integer, ActionStatsLatestRecord> records =
                        aggregator.createRecords(ImmutableMap.of(subgroupService.key(), subgroupService,
                                                                 subgroupBA.key(), subgroupBA),
                                                 ImmutableMap.of(ACTION_GROUP_KEY, ACTION_GROUP))
                                        .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        final ActionStatsLatestRecord serviceRecord = records.get(subgroupService.id());
        assertThat(serviceRecord.getTotalActionCount(), is(1));
        assertThat(serviceRecord.getTotalEntityCount(), is(1));
        assertThat(serviceRecord.getMgmtUnitSubgroupId(), is(subgroupService.id()));
        assertThat(serviceRecord.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(serviceRecord.getTotalSavings().doubleValue(), closeTo(action.getSavingsPerHour().getAmount(), 0.001));
        assertThat(serviceRecord.getTotalInvestment().doubleValue(), is(0.0));

        final ActionStatsLatestRecord baRecord = records.get(subgroupBA.id());
        assertThat(baRecord.getTotalActionCount(), is(1));
        assertThat(baRecord.getTotalEntityCount(), is(1));
        assertThat(baRecord.getMgmtUnitSubgroupId(), is(subgroupBA.id()));
        assertThat(baRecord.getActionGroupId(), is(ACTION_GROUP.id()));
        assertThat(baRecord.getTotalSavings().doubleValue(), closeTo(action.getSavingsPerHour().getAmount(), 0.001));
        assertThat(baRecord.getTotalInvestment().doubleValue(), is(0.0));
    }

    /**
     * Check that for uninvolved entity type record is not created.
     */
    @Test
    public void testUninvolvedEntityType() {
        final ActionDTO.ActionEntity pm = ActionDTO.ActionEntity.newBuilder()
                        .setType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                        .setId(INVOLVED_PM_ENTITY_ID)
                        .build();

        final ImmutableStatsActionView actionSnapshot = ImmutableStatsActionView.builder()
                        .actionGroupKey(ACTION_GROUP_KEY)
                        .recommendation(action)
                        .primaryEntity(pm)
                        .build();

        final PropagatedActionAggregator aggregator = aggregatorFactory.newAggregator(TIME);
        aggregator.processAction(actionSnapshot, new LiveActionsStatistician.PreviousBroadcastActions());

        final Map<Integer, ActionStatsLatestRecord> records =
                        aggregator.createRecords(ImmutableMap.of(subgroupBT.key(), subgroupBT),
                                                 ImmutableMap.of(ACTION_GROUP_KEY, ACTION_GROUP))
                                        .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        Assert.assertTrue(records.isEmpty());
    }
}
