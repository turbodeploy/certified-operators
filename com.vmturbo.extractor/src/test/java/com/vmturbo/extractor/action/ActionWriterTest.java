package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.Status;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse.ActionChunk;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
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
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Unit tests for the {@link ActionWriter}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class ActionWriterTest {

    private static final long ACTION_WRITING_INTERVAL_MS = 10_000;

    private static final long REALTIME_CONTEXT = 777;

    private static final ActionSpec ACTION = ActionSpec.newBuilder()
            .setExplanation("foo")
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

    private ActionsServiceMole actionsBackend = spy(ActionsServiceMole.class);

    /**
     * Test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsBackend);

    private ExecutorService executorService = mock(ExecutorService.class);

    private ActionWriter actionWriter;

    private List<Record> actionSpecUpsertCapture;
    private List<Record> actionSpecUpdateCapture;
    private List<Record> actionInsertCapture;

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

        actionWriter = spy(new ActionWriter(clock, executorService,
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                endpoint,
                writerConfig,
                actionConverter,
                actionHashManager,
                ACTION_WRITING_INTERVAL_MS, TimeUnit.MILLISECONDS,
                true,
                REALTIME_CONTEXT));
        doReturn(entitiesUpserterSink).when(actionWriter).getActionSpecUpsertSink(
                any(DSLContext.class), any(), any());
        doReturn(entitiesUpdaterSink).when(actionWriter).getActionSpecUpdaterSink(
                any(DSLContext.class), any(), any(), any());
        doReturn(metricInserterSink).when(actionWriter).getActionInserterSink(any(DSLContext.class));

        when(actionsBackend.getAllActions(any())).thenReturn(Collections.singletonList(
            FilteredActionResponse.newBuilder()
                .setActionChunk(ActionChunk.newBuilder()
                    .addActions(ActionOrchestratorAction.newBuilder()
                        .setActionSpec(ACTION)))
                .build()));
    }

    /**
     * Test the typical action writing cycle.
     */
    @Test
    public void testWriteActions() {
        final long specOid = 999;
        Record actionSpecRecord = new Record(ActionModel.ActionSpec.TABLE);
        actionSpecRecord.set(ActionModel.ActionSpec.SPEC_OID, specOid);

        Record actionRecord = new Record(ActionMetric.TABLE);
        actionRecord.set(ActionMetric.STATE, ActionState.IN_PROGRESS);
        actionRecord.set(ActionMetric.ACTION_SPEC_OID, specOid);
        actionRecord.set(ActionMetric.ACTION_OID, 888L);
        actionRecord.set(ActionMetric.USER, "me");

        when(actionConverter.makeActionSpecRecord(ACTION)).thenReturn(actionSpecRecord);
        when(actionConverter.makeActionRecord(ACTION, actionSpecRecord)).thenReturn(actionRecord);

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
        when(actionHashManager.open(clock.millis())).thenReturn(snapshotManager);

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

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

    /**
     * Test that plan contexts get ignored.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testIgnorePlanContext() throws Exception {
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT + 100));

        verify(actionWriter, never()).writeActions();
    }

    /**
     * Test that the action writing interval is respected per context.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSkipUpdate() throws Exception {
        doAnswer(invocation -> {
            return null;
        }).when(actionWriter).writeActions();

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter, times(1)).writeActions();

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
        // Still written just once.
        verify(actionWriter, times(1)).writeActions();

        // A buy RI plan should still get processed.
        actionWriter.onActionsUpdated(ActionsUpdated.newBuilder()
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                        .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                                .setTopologyContextId(REALTIME_CONTEXT)))
                .build());
        verify(actionWriter, times(2)).writeActions();

        clock.addTime(ACTION_WRITING_INTERVAL_MS - 1, ChronoUnit.MILLIS);

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter, times(2)).writeActions();

        clock.addTime(1, ChronoUnit.MILLIS);

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter, times(3)).writeActions();
    }

    /**
     * Test that an AO exception doesn't get propagated to the caller.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testAOException() {
        when(actionsBackend.getAllActionsError(any())).thenReturn(Optional.of(Status.INTERNAL.asException()));
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
    }

    /**
     * Test that a unsupported dialect exception doesn't get propagated to the caller.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUnsupportedDialectException() throws Exception {
        doThrow(new UnsupportedDialectException("bad!")).when(actionWriter).writeActions();
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
    }

    /**
     * Test that a SQL exception doesn't get propagated to the caller.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSQLException() throws Exception {
        doThrow(new SQLException("bad!", "SQL:FOO", 123)).when(actionWriter).writeActions();
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
    }

    /**
     * Test that disabling ingestion works.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDisableIngestion() throws Exception {
        ActionWriter actionWriter = spy(new ActionWriter(clock, executorService,
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                mock(DbEndpoint.class),
                writerConfig,
                actionConverter,
                actionHashManager,
                ACTION_WRITING_INTERVAL_MS, TimeUnit.MILLISECONDS,
                false,
                REALTIME_CONTEXT));
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter, never()).writeActions();
    }

    private ActionsUpdated actionsUpdated(final long context) {
        return ActionsUpdated.newBuilder()
            .setActionPlanInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(1)
                        .setTopologyContextId(context))))
            .build();
    }
}
