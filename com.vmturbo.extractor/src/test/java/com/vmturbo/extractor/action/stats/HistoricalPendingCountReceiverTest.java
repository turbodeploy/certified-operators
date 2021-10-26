package com.vmturbo.extractor.action.stats;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.ActionGroup;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.ActionRollupNotification;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.HourlyActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.models.ActionStatsModel;
import com.vmturbo.extractor.models.ActionStatsModel.PendingActionStats;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Unit tests for the {@link HistoricalPendingCountReceiver}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class HistoricalPendingCountReceiverTest {

    /**
     * The class under test.
     */
    private HistoricalPendingCountReceiver historicalPendingCountReceiver;

    /**
     * Mock ExtractorFeatureFlags to indicate that this feature is enabled.
     */
    private ExtractorFeatureFlags extractorFeatureFlagsMock = Mockito.mock(ExtractorFeatureFlags.class);

    /**
     * Mock DbEndpoint.
     */
    private DbEndpoint dbEndpointMock = Mockito.mock(DbEndpoint.class);

    /**
     * Mock ExecutorService.
     */
    private ExecutorService poolMock = Mockito.mock(ExecutorService.class);

    /**
     * Mock WriterConfig.
     */
    private WriterConfig writerConfigMock = Mockito.mock(WriterConfig.class);

    /**
     * A capture of the records that would be written to the database.
     */
    private List<Record> pendingActionStatsRecordsCapture;

    /**
     * Set up the mocks.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Before
    public void setup() throws SQLException, InterruptedException {
        when(extractorFeatureFlagsMock.isReportingActionIngestionEnabled()).thenReturn(true);

        // Spy so that we can record the results written to the database (via the sink).
        // The alternative to this would be to modify the production code to use a functional
        // interface for creating the sink, similar to SinkFactory in CompletedActionWriter.
        historicalPendingCountReceiver = Mockito.spy(
                new HistoricalPendingCountReceiver(extractorFeatureFlagsMock,
                        dbEndpointMock,
                        poolMock,
                        writerConfigMock));

        // Create the mock sink to capture the results that would be sent to the database
        DslRecordSink upsertSinkMock = mock(DslUpsertRecordSink.class);
        pendingActionStatsRecordsCapture = captureSink(upsertSinkMock, false);
        // Activate the spy to ensure the records are captured
        doReturn(upsertSinkMock).when(historicalPendingCountReceiver)
                .getUpsertSink(any(DSLContext.class), eq(PendingActionStats.TABLE), any(), any());

    }

    /**
     * Test that the receiver can accept and process an action stats rollup message.
     */
    @Test
    public void testAcceptRollup() {
        // prepare
        ActionRollupNotification rollupNotification = ActionRollupNotification.newBuilder()
                .addHourlyActionStats(HourlyActionStat.newBuilder()
                        .setHourTime(0)
                        .setScopeOid(1234L)
                        .setScopeEntityType(10)
                        .setScopeEnvironmentType(EnvironmentType.CLOUD)
                        .setActionGroupId(0)
                        .setPriorActionCount(77)
                        .setClearedActionCount(2)
                        .setNewActionCount(10)
                        .setEntityCount(4)
                        .setSavings(100.03f)
                        .setInvestments(0.0f))
                .build();

        // act
        historicalPendingCountReceiver.accept(rollupNotification);

        // verify
        assertEquals(1, pendingActionStatsRecordsCapture.size());
        Record record = pendingActionStatsRecordsCapture.get(0);
        assertEquals(1234L, record.get(PendingActionStats.SCOPE_OID), 0.0d);
        assertEquals(EntityType.VIRTUAL_MACHINE, record.get(PendingActionStats.ENTITY_TYPE));
        assertEquals(com.vmturbo.extractor.schema.enums.EnvironmentType.CLOUD,
                record.get(PendingActionStats.ENVIRONMENT_TYPE));
        assertEquals(0, (int)record.get(PendingActionStats.ACTION_GROUP));
        assertEquals(77, (int)record.get(PendingActionStats.PRIOR_ACTION_COUNT));
        assertEquals(2, (int)record.get(PendingActionStats.CLEARED_ACTION_COUNT));
        assertEquals(10, (int)record.get(PendingActionStats.NEW_ACTION_COUNT));
        assertEquals(4, (int)record.get(PendingActionStats.INVOLVED_ENTITY_COUNT));
        assertEquals(100.03f, record.get(PendingActionStats.SAVINGS), 0.0d);
        assertEquals(0.0f, record.get(PendingActionStats.INVESTMENTS), 0.0d);
    }

    /**
     * Test that the receiver can accept and process an action stats rollup message.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test
    public void testAcceptRollupWithActionGroup() throws SQLException, InterruptedException {
        // prepare
        ActionRollupNotification rollupNotification = ActionRollupNotification.newBuilder()
                .addHourlyActionStats(HourlyActionStat.newBuilder()
                        .setHourTime(0)
                        .setScopeOid(1234L)
                        .setScopeEntityType(10)
                        .setScopeEnvironmentType(EnvironmentType.CLOUD)
                        .setActionGroupId(0)
                        .setPriorActionCount(77)
                        .setClearedActionCount(2)
                        .setNewActionCount(10)
                        .setEntityCount(4)
                        .setSavings(100.03f)
                        .setInvestments(0.0f))
                .addActionGroups(ActionGroup.newBuilder()
                        .setId(765)
                        .setActionType(ActionType.SCALE)
                        .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT)
                        .setActionMode(ActionMode.MANUAL)
                        .setActionState(ActionState.READY)
                        .addRiskDescriptions("Scaling to improve efficiency."))
                .build();

        // Create an additonal mock to capture the results written to the action groups table
        // Create the mock sink to capture the results that would be sent to the database
        final DslRecordSink actionGroupSink = mock(DslUpsertRecordSink.class);
        final List<Record> actionGroupRecordsCapture = captureSink(actionGroupSink, false);
        // Activate the spy to ensure the records are captured
        doReturn(actionGroupSink).when(historicalPendingCountReceiver)
                .getUpsertSink(any(DSLContext.class), eq(ActionStatsModel.ActionGroup.TABLE), any(),
                        any());

        // act
        historicalPendingCountReceiver.accept(rollupNotification);

        // verify
        assertEquals(1, pendingActionStatsRecordsCapture.size());
        Record record = pendingActionStatsRecordsCapture.get(0);
        assertEquals(1234L, record.get(PendingActionStats.SCOPE_OID), 0.0d);
        assertEquals(EntityType.VIRTUAL_MACHINE, record.get(PendingActionStats.ENTITY_TYPE));
        assertEquals(com.vmturbo.extractor.schema.enums.EnvironmentType.CLOUD,
                record.get(PendingActionStats.ENVIRONMENT_TYPE));
        assertEquals(0, (int)record.get(PendingActionStats.ACTION_GROUP));
        assertEquals(77, (int)record.get(PendingActionStats.PRIOR_ACTION_COUNT));
        assertEquals(2, (int)record.get(PendingActionStats.CLEARED_ACTION_COUNT));
        assertEquals(10, (int)record.get(PendingActionStats.NEW_ACTION_COUNT));
        assertEquals(4, (int)record.get(PendingActionStats.INVOLVED_ENTITY_COUNT));
        assertEquals(100.03f, record.get(PendingActionStats.SAVINGS), 0.0d);
        assertEquals(0.0f, record.get(PendingActionStats.INVESTMENTS), 0.0d);
        assertEquals(1, actionGroupRecordsCapture.size());
        Record actionGroupRecord = actionGroupRecordsCapture.get(0);
        assertEquals(765, (int)actionGroupRecord.get(ActionStatsModel.ActionGroup.ID));
        assertEquals(com.vmturbo.extractor.schema.enums.ActionType.SCALE,
                actionGroupRecord.get(ActionStatsModel.ActionGroup.TYPE));
        assertEquals(com.vmturbo.extractor.schema.enums.ActionCategory.EFFICIENCY_IMPROVEMENT,
                actionGroupRecord.get(ActionStatsModel.ActionGroup.CATEGORY));
        assertEquals(com.vmturbo.extractor.schema.enums.ActionMode.MANUAL,
                actionGroupRecord.get(ActionStatsModel.ActionGroup.MODE));
        assertEquals(com.vmturbo.extractor.schema.enums.ActionState.READY,
                actionGroupRecord.get(ActionStatsModel.ActionGroup.STATE));
        assertArrayEquals(new String[]{"Scaling to improve efficiency."},
                actionGroupRecord.get(ActionStatsModel.ActionGroup.RISKS));
    }
}
