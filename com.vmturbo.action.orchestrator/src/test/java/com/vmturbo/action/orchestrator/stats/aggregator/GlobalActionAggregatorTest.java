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
import com.vmturbo.action.orchestrator.stats.ImmutableSingleActionSnapshot;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.SingleActionSnapshotFactory.SingleActionSnapshot;
import com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator.GlobalAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class GlobalActionAggregatorTest {

    private static final ActionDTO.Action SAVINGS_ACTION = ActionDTO.Action.newBuilder()
            .setId(1)
            .setInfo(ActionInfo.getDefaultInstance())
            .setImportance(1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

    private static final LocalDateTime TIME = LocalDateTime.MAX;

    private final GlobalAggregatorFactory aggregatorFactory = new GlobalAggregatorFactory();

    @Test
    public void testGlobalAndPerEntityRecords() {
        final GlobalActionAggregator aggregator = aggregatorFactory.newAggregator(TIME);

        final int actionGroupId = 123;
        final ActionGroupKey actionGroupKey = mock(ActionGroupKey.class);
        final ActionGroup actionGroup = mock(ActionGroup.class);
        when(actionGroup.id()).thenReturn(actionGroupId);
        SingleActionSnapshot savingsSnapshot = ImmutableSingleActionSnapshot.builder()
                .actionGroupKey(actionGroupKey)
                .recommendation(SAVINGS_ACTION)
                .addInvolvedEntities(ActionEntity.newBuilder()
                        .setId(7)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                .addInvolvedEntities(ActionEntity.newBuilder()
                        .setId(77)
                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .build())
                .build();

        aggregator.processAction(savingsSnapshot);

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
    }
}
