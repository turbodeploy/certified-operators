package com.vmturbo.action.orchestrator.stats.rollup.export;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.ActionRollupNotification;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.HourlyActionStat;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableRolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableRolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.export.RollupExporter.RollupExportWorker;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.MutableFixedClock;

/**
 * The rollup exporter test class.
 */
public class RollupExporterTest {

    private ActionGroupStore actionGroupStore = mock(ActionGroupStore.class);
    private MgmtUnitSubgroupStore mgmtUnitSubgroupStore = mock(MgmtUnitSubgroupStore.class);

    @Captor
    private ArgumentCaptor<Runnable> runnableCaptor;

    @Captor
    private ArgumentCaptor<ActionRollupNotification> notificationArgumentCaptor;

    @Mock
    private IMessageSender<ActionRollupNotification> requestSender;

    private final ActionStatTable.Reader fromReader = mock(ActionStatTable.Reader.class);

    private final ExecutorService executorService = mock(ExecutorService.class);

    private static final String MEM_CONGESTION = "Mem congestion";
    private static final String CPU_CONGESTION = "CPU congestion";

    private static final LocalDateTime START_TIME =
            LocalDateTime.ofEpochSecond(10000, 100, ZoneOffset.UTC);

    private RollupExporter rollupExporter;
    private RollupExportWorker exportWorker;

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private static final MgmtUnitSubgroup MUS_1 = ImmutableMgmtUnitSubgroup.builder()
        .id(222)
        .key(ImmutableMgmtUnitSubgroupKey.builder()
            .entityType(1)
            .environmentType(EnvironmentType.CLOUD)
            .mgmtUnitType(ManagementUnitType.CLUSTER)
            .mgmtUnitId(777)
            .build())
        .build();

    private static final MgmtUnitSubgroup MUS_NO_ENTITY_TYPE = ImmutableMgmtUnitSubgroup.builder()
            .id(222)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                    .environmentType(EnvironmentType.ON_PREM)
                    .mgmtUnitType(ManagementUnitType.GLOBAL)
                    .mgmtUnitId(0)
                    .build())
            .build();

    private static final ActionGroup ACTION_GROUP_1 = ImmutableActionGroup.builder()
            .id(11)
            .key(ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.DISABLED)
                .actionState(ActionState.ACCEPTED)
                .actionType(ActionType.ACTIVATE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .addActionRelatedRisk(MEM_CONGESTION)
                .addActionRelatedRisk(CPU_CONGESTION)
                .build())
            .build();

    private static final ActionGroup ACTION_GROUP_2 = ImmutableActionGroup.builder()
            .id(11)
            .key(ImmutableActionGroupKey.builder()
                .actionMode(ActionMode.MANUAL)
                .actionState(ActionState.ACCEPTED)
                .actionType(ActionType.MOVE)
                .category(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .addActionRelatedRisk(CPU_CONGESTION)
                .build())
            .build();

    /**
     * Initializes the tests.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        rollupExporter = new RollupExporter(requestSender, mgmtUnitSubgroupStore,
                actionGroupStore, clock, true, executorService);
        verify(executorService).submit(runnableCaptor.capture());
        exportWorker = (RollupExportWorker)runnableCaptor.getValue();
    }

    /**
     * Run rollup export test.
     *
     * @throws CommunicationException if an error occurred during rollup export.
     * @throws InterruptedException if the thread is killed during rollup export.
     */
    @Test
    public void testRollupExport() throws CommunicationException, InterruptedException {
        when(mgmtUnitSubgroupStore.getGroupsById(Collections.singleton(MUS_1.id())))
            .thenReturn(Collections.singletonMap(MUS_1.id(), MUS_1));

        when(actionGroupStore.getGroupsById(Collections.singleton(ACTION_GROUP_1.id())))
            .thenReturn(Collections.singletonMap(ACTION_GROUP_1.id(), ACTION_GROUP_1));

        int priorActionCount = 1;
        int newActionCount = 2;
        int avgEntityCount = 3;
        float avgSavings = 4;
        float avgInvestment = 5;
        int numSnapshots = 3;
        final RolledUpActionGroupStat agStat = agStat(priorActionCount, newActionCount, avgEntityCount, avgSavings, avgInvestment);
        final RolledUpActionStats rolledUpActionStats = ImmutableRolledUpActionStats.builder()
                .startTime(LocalDateTime.now(clock))
                .numActionSnapshots(numSnapshots)
                .putStatsByActionGroupId(ACTION_GROUP_1.id(), agStat)
                .build();

        rollupExporter.exportRollup(MUS_1.id(), rolledUpActionStats);

        exportWorker.runIteration();

        verify(requestSender).sendMessage(notificationArgumentCaptor.capture());
        ActionRollupNotification notification = notificationArgumentCaptor.getValue();
        assertThat(notification.getActionGroupsList().size(), is(1));
        ActionRollupExport.ActionGroup protoAg = notification.getActionGroupsList().get(0);
        assertActionGroupEqual(protoAg, ACTION_GROUP_1);

        assertThat(notification.getHourlyActionStatsList().size(), is(1));
        HourlyActionStat stat = notification.getHourlyActionStatsList().get(0);

        assertThat(stat.getActionGroupId(), is(ACTION_GROUP_1.id()));
        // Not implemented yet.
        assertThat(stat.getClearedActionCount(), is(0));
        assertThat(stat.getEntityCount(), is(avgEntityCount * 3));
        assertThat(stat.getSavings(), is(avgSavings * 3));
        assertThat(stat.getInvestments(), is(avgInvestment * 3));
        assertThat(stat.getNewActionCount(), is(newActionCount));
        assertThat(stat.getPriorActionCount(), is(priorActionCount));
        assertThat(stat.getHourTime(), is(clock.millis()));
        assertThat(stat.getScopeOid(), is(MUS_1.key().mgmtUnitId()));
        assertThat(stat.getScopeEntityType(), is(MUS_1.key().entityType().get()));
        assertThat(stat.getScopeEnvironmentType(), is(MUS_1.key().environmentType()));
    }

    private void assertActionGroupEqual(ActionRollupExport.ActionGroup proto, ActionGroup src) {
        assertThat(proto.getActionType(), is(src.key().getActionType()));
        assertThat(proto.getActionCategory(), is(src.key().getCategory()));
        assertThat(proto.getActionSeverity(), is(ActionView.categoryToSeverity(src.key().getCategory())));
        assertThat(proto.getId(), is(src.id()));
        assertThat(new HashSet<>(proto.getRiskDescriptionsList()), is(src.key().getActionRelatedRisk()));
    }

    private RolledUpActionGroupStat agStat(int priorActionCount, int newActionCount,
            double avgEntityCount, double avgSavings, double avgInvestment) {
        return ImmutableRolledUpActionGroupStat.builder()
                .avgActionCount(0.5)
                .maxActionCount(1)
                .minActionCount(0)
                .newActionCount(newActionCount)
                .priorActionCount(priorActionCount)
                .avgInvestment(avgInvestment)
                .maxInvestment(1)
                .minInvestment(0.1)
                .avgSavings(avgSavings)
                .maxSavings(1.1)
                .minSavings(0.2)
                .avgEntityCount(avgEntityCount)
                .minEntityCount(1)
                .maxEntityCount(3)
                .build();
    }
}
