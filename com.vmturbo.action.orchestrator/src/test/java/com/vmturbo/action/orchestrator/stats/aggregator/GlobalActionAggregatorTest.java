package com.vmturbo.action.orchestrator.stats.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.ImmutableStatsActionView;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician.PreviousBroadcastActions;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
import com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator.GlobalAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class GlobalActionAggregatorTest {

    private static final LocalDateTime TIME = LocalDateTime.MAX;

    private final GlobalAggregatorFactory aggregatorFactory = new GlobalAggregatorFactory();

    private final ActionGroupKey actionGroupKey = ImmutableActionGroupKey.builder()
        .actionType(ActionType.MOVE)
        .actionMode(ActionMode.RECOMMEND)
        .category(ActionCategory.PERFORMANCE_ASSURANCE)
        .actionState(ActionState.READY)
        .actionRelatedRisk("Mem congestion")
        .build();

    @Test
    public void testOnPremGlobalAndPerEntityRecords() {
        final ActionDTO.Action onPremSavingsAction = Action.newBuilder()
            .setId(1)
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(7)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.ON_PREM))))
            .setDeprecatedImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

        final GlobalActionAggregator aggregator = aggregatorFactory.newAggregator(TIME);

        final int actionGroupId = 123;
        final ActionGroup actionGroup = mock(ActionGroup.class);
        when(actionGroup.id()).thenReturn(actionGroupId);
        StatsActionView savingsSnapshot = ImmutableStatsActionView.builder()
            .actionGroupKey(actionGroupKey)
            .recommendation(onPremSavingsAction)
            .addInvolvedEntities(ActionEntity.newBuilder()
                .setId(7)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build())
            .addInvolvedEntities(ActionEntity.newBuilder()
                .setId(77)
                .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                .build())
            .build();

        aggregator.processAction(savingsSnapshot, new PreviousBroadcastActions());

        final MgmtUnitSubgroup globalSubgroup = ImmutableMgmtUnitSubgroup.builder()
                .id(987)
                .key(ImmutableMgmtUnitSubgroupKey.builder()
                    // No entity type.
                    .environmentType(EnvironmentType.ON_PREM)
                    .mgmtUnitType(ManagementUnitType.GLOBAL)
                    .mgmtUnitId(0)
                    .build())
                .build();
        final MgmtUnitSubgroup vmSubgroup = ImmutableMgmtUnitSubgroup.builder()
            .id(321)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .environmentType(EnvironmentType.ON_PREM)
                .mgmtUnitType(ManagementUnitType.GLOBAL)
                .mgmtUnitId(0)
                .build())
            .build();
        final MgmtUnitSubgroup pmSubgroup = ImmutableMgmtUnitSubgroup.builder()
            .id(432)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .environmentType(EnvironmentType.ON_PREM)
                .mgmtUnitType(ManagementUnitType.GLOBAL)
                .mgmtUnitId(0)
                .build())
            .build();

        // Expect one record for VM, and one record for PM
        final Map<Integer, ActionStatsLatestRecord> recordsByMgmtUnitSubgroup = aggregator.createRecords(
                ImmutableMap.of(globalSubgroup.key(), globalSubgroup, vmSubgroup.key(), vmSubgroup, pmSubgroup.key(), pmSubgroup),
                ImmutableMap.of(actionGroupKey, actionGroup))
            .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        ActionStatsLatestRecord vmRecord = recordsByMgmtUnitSubgroup.get(vmSubgroup.id());
        assertThat(vmRecord.getTotalEntityCount(), is(1));
        assertThat(vmRecord.getTotalActionCount(), is(1));

        ActionStatsLatestRecord pmRecord = recordsByMgmtUnitSubgroup.get(pmSubgroup.id());
        assertThat(pmRecord.getTotalEntityCount(), is(1));
        assertThat(pmRecord.getTotalActionCount(), is(1));

        ActionStatsLatestRecord globalRecord = recordsByMgmtUnitSubgroup.get(globalSubgroup.id());
        assertThat(globalRecord.getTotalEntityCount(), is(2));
        assertThat(globalRecord.getTotalActionCount(), is(1));
        assertThat(globalRecord.getNewActionCount(), is(0));
    }

    @Test
    public void testCloudGlobalAndPerEntityRecords() {
        final ActionEntity entity = ActionEntity.newBuilder()
            .setId(7)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();
        final ActionDTO.Action cloudSavingsAction = Action.newBuilder()
            .setId(1)
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(entity)))
            .setDeprecatedImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

        final GlobalActionAggregator aggregator = aggregatorFactory.newAggregator(TIME);

        final int actionGroupId = 123;
        final ActionGroup actionGroup = mock(ActionGroup.class);
        when(actionGroup.id()).thenReturn(actionGroupId);
        StatsActionView savingsSnapshot = ImmutableStatsActionView.builder()
            .actionGroupKey(actionGroupKey)
            .recommendation(cloudSavingsAction)
            .addInvolvedEntities(entity)
            .build();

        aggregator.processAction(savingsSnapshot, new PreviousBroadcastActions());

        final MgmtUnitSubgroup globalSubgroup = ImmutableMgmtUnitSubgroup.builder()
            .id(987)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                // No entity type.
                .environmentType(EnvironmentType.CLOUD)
                .mgmtUnitType(ManagementUnitType.GLOBAL)
                .mgmtUnitId(0)
                .build())
            .build();
        final MgmtUnitSubgroup vmSubgroup = ImmutableMgmtUnitSubgroup.builder()
            .id(321)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .environmentType(EnvironmentType.CLOUD)
                .mgmtUnitType(ManagementUnitType.GLOBAL)
                .mgmtUnitId(0)
                .build())
            .build();

        // Expect one record for VM, and one record for PM
        final Map<Integer, ActionStatsLatestRecord> recordsByMgmtUnitSubgroup = aggregator.createRecords(
            ImmutableMap.of(globalSubgroup.key(), globalSubgroup, vmSubgroup.key(), vmSubgroup),
            ImmutableMap.of(actionGroupKey, actionGroup))
            .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        ActionStatsLatestRecord vmRecord = recordsByMgmtUnitSubgroup.get(vmSubgroup.id());
        assertThat(vmRecord.getTotalEntityCount(), is(1));
        assertThat(vmRecord.getTotalActionCount(), is(1));

        ActionStatsLatestRecord globalRecord = recordsByMgmtUnitSubgroup.get(globalSubgroup.id());
        assertThat(globalRecord.getTotalEntityCount(), is(1));
        assertThat(globalRecord.getTotalActionCount(), is(1));
        assertThat(globalRecord.getNewActionCount(), is(0));
    }

    /**
     * Tests our handling of "hybrid" (i.e. cross-environment actions).
     * Note that right now we consider "hybrid" actions to be fully on-prem, which isn't really
     * correct. However, we don't expect to see "hybrid" actions in the realtime environment
     * (which is where we aggregate stats).
     */
    @Test
    public void testHybridGlobalAndPerEntityRecords() {
        final ActionEntity app = ActionEntity.newBuilder()
            .setId(7)
            .setType(EntityType.APPLICATION_COMPONENT_VALUE)
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .build();
        final ActionEntity onPremVm = ActionEntity.newBuilder()
            .setId(8)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .build();
        final ActionEntity cloudVm = ActionEntity.newBuilder()
            .setId(9)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

        final ActionDTO.Action hybridAction = Action.newBuilder()
            .setId(1)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(app)
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(onPremVm)
                        .setDestination(cloudVm))))
            .setDeprecatedImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

        final GlobalActionAggregator aggregator = aggregatorFactory.newAggregator(TIME);

        final int actionGroupId = 123;
        final ActionGroup actionGroup = mock(ActionGroup.class);
        when(actionGroup.id()).thenReturn(actionGroupId);
        StatsActionView savingsSnapshot = ImmutableStatsActionView.builder()
            .actionGroupKey(actionGroupKey)
            .recommendation(hybridAction)
            .addInvolvedEntities(app)
            .addInvolvedEntities(onPremVm)
            .addInvolvedEntities(cloudVm)
            .build();

        aggregator.processAction(savingsSnapshot, new PreviousBroadcastActions());

        final MgmtUnitSubgroup globalSubgroup = ImmutableMgmtUnitSubgroup.builder()
            .id(987)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                // No entity type.
                .environmentType(EnvironmentType.ON_PREM)
                .mgmtUnitType(ManagementUnitType.GLOBAL)
                .mgmtUnitId(0)
                .build())
            .build();
        final MgmtUnitSubgroup appCompSubgroup = ImmutableMgmtUnitSubgroup.builder()
            .id(231)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .environmentType(EnvironmentType.ON_PREM)
                .mgmtUnitType(ManagementUnitType.GLOBAL)
                .mgmtUnitId(0)
                .build())
            .build();
        final MgmtUnitSubgroup vmSubgroup = ImmutableMgmtUnitSubgroup.builder()
            .id(321)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .environmentType(EnvironmentType.ON_PREM)
                .mgmtUnitType(ManagementUnitType.GLOBAL)
                .mgmtUnitId(0)
                .build())
            .build();

        // Expect one record for VM, and one record for PM
        final Map<Integer, ActionStatsLatestRecord> recordsByMgmtUnitSubgroup = aggregator.createRecords(
            ImmutableMap.of(globalSubgroup.key(), globalSubgroup, vmSubgroup.key(), vmSubgroup,
                    appCompSubgroup.key(), appCompSubgroup),
            ImmutableMap.of(actionGroupKey, actionGroup))
            .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        ActionStatsLatestRecord vmRecord = recordsByMgmtUnitSubgroup.get(vmSubgroup.id());
        assertThat(vmRecord.getTotalEntityCount(), is(2));
        assertThat(vmRecord.getTotalActionCount(), is(1));

        ActionStatsLatestRecord appRecord = recordsByMgmtUnitSubgroup.get(appCompSubgroup.id());
        assertThat(appRecord.getTotalEntityCount(), is(1));
        assertThat(appRecord.getTotalActionCount(), is(1));

        ActionStatsLatestRecord globalRecord = recordsByMgmtUnitSubgroup.get(globalSubgroup.id());
        assertThat(globalRecord.getTotalEntityCount(), is(3));
        assertThat(globalRecord.getTotalActionCount(), is(1));
        assertThat(globalRecord.getNewActionCount(), is(0));
    }
}
