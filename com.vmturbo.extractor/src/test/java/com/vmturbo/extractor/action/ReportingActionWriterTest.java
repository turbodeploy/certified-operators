package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.RecordHashManager.SnapshotManager;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.ActionMetric;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Unit tests for the {@link ReportingActionWriter}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class ReportingActionWriterTest {

    private static final long ACTION_WRITING_INTERVAL_MS = 10_000;

    private static final ActionSpec ACTION_SPEC = ActionSpec.newBuilder()
            .setExplanation("foo").build();

    private static final ActionOrchestratorAction ACTION = ActionOrchestratorAction.newBuilder()
            .setActionId(1121L)
            .setActionSpec(ACTION_SPEC)
            .build();

    @Autowired
    private ExtractorDbConfig dbConfig;

    private WriterConfig writerConfig = ImmutableWriterConfig.builder()
            .lastSeenUpdateIntervalMinutes(1)
            .lastSeenAdditionalFuzzMinutes(1)
            .insertTimeoutSeconds(10)
            .build();

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private ActionConverter actionConverter = mock(ActionConverter.class);

    private ActionHashManager actionHashManager = mock(ActionHashManager.class);

    private DataProvider dataProvider = mock(DataProvider.class);

    private ExecutorService executorService = mock(ExecutorService.class);

    private ReportingActionWriter actionWriter;

    private List<Record> actionSpecUpsertCapture;
    private List<Record> actionSpecUpdateCapture;
    private List<Record> actionInsertCapture;
    private static final Logger logger = LogManager.getLogger();

    private final MultiStageTimer timer = new MultiStageTimer(logger);

    /**
     * Common setup code before each test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslRecordSink entitiesUpserterSink = mock(DslUpsertRecordSink.class);
        this.actionSpecUpsertCapture = captureSink(entitiesUpserterSink, false);
        DslRecordSink entitiesUpdaterSink = mock(DslUpdateRecordSink.class);
        this.actionSpecUpdateCapture = captureSink(entitiesUpdaterSink, false);
        DslRecordSink metricInserterSink = mock(DslRecordSink.class);
        this.actionInsertCapture = captureSink(metricInserterSink, false);

        actionWriter = spy(new ReportingActionWriter(
                clock,
                executorService,
                endpoint,
                writerConfig,
                actionConverter,
                actionHashManager,
                ACTION_WRITING_INTERVAL_MS));
        doReturn(entitiesUpserterSink).when(actionWriter).getActionSpecUpsertSink(
                any(DSLContext.class), any(), any());
        doReturn(entitiesUpdaterSink).when(actionWriter).getActionSpecUpdaterSink(
                any(DSLContext.class), any(), any(), any());
        doReturn(metricInserterSink).when(actionWriter).getActionInserterSink(any(DSLContext.class));
        doAnswer(inv -> null).when(dataProvider).getTopologyGraph();
    }

    /**
     * Test the typical action writing cycle for reporting.
     *
     * @throws UnsupportedDialectException if the type of endpoint is unsupported
     * @throws InterruptedException if interrupted
     * @throws SQLException if there's a problem using the db endpoint
     */
    @Test
    public void testWriteActionsForReporting()
            throws UnsupportedDialectException, InterruptedException, SQLException {
        final long specOid = 999;
        Record actionSpecRecord = new Record(ActionModel.ActionSpec.TABLE);
        actionSpecRecord.set(ActionModel.ActionSpec.SPEC_OID, specOid);

        Record actionRecord = new Record(ActionMetric.TABLE);
        actionRecord.set(ActionMetric.STATE, ActionState.IN_PROGRESS);
        actionRecord.set(ActionMetric.ACTION_SPEC_OID, specOid);
        actionRecord.set(ActionMetric.ACTION_OID, 888L);
        actionRecord.set(ActionMetric.USER, "me");

        when(actionConverter.makeActionSpecRecord(ACTION_SPEC)).thenReturn(actionSpecRecord);
        when(actionConverter.makeActionRecord(ACTION_SPEC, actionSpecRecord)).thenReturn(actionRecord);

        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        long hash = 1728;
        when(snapshotManager.updateRecordHash(actionSpecRecord)).thenReturn(hash);
        when(actionHashManager.getEntityHash(specOid)).thenReturn(hash);

        doAnswer(invocation -> {
            Record record = invocation.getArgumentAt(0, Record.class);
            record.set(ActionModel.ActionSpec.FIRST_SEEN, new Timestamp(clock.millis() - 100_000));
            record.set(ActionModel.ActionSpec.LAST_SEEN, new Timestamp(clock.millis()));
            return null;
        }).when(snapshotManager).setRecordTimes(actionSpecRecord);
        when(actionHashManager.open(anyLong())).thenReturn(snapshotManager);

        // accept action and write
        actionWriter.accept(ACTION);
        actionWriter.write(new HashMap<>(), TypeInfoCase.MARKET, timer);

        assertThat(actionSpecUpsertCapture.get(0).asMap(), is(actionSpecRecord.asMap()));
        assertThat(actionSpecUpdateCapture, is(empty()));
        assertThat(actionInsertCapture.get(0).asMap(), is(actionRecord.asMap()));

        // Verify the hash is being set.
        assertThat(actionSpecUpsertCapture.get(0).get(ActionModel.ActionSpec.HASH), is(hash));
        assertThat(actionInsertCapture.get(0).get(ActionMetric.ACTION_SPEC_HASH), is(hash));

        verify(snapshotManager).updateRecordHash(actionSpecRecord);
        verify(snapshotManager).setRecordTimes(actionSpecUpsertCapture.get(0));
        verify(snapshotManager).processChanges(any());
    }
}
