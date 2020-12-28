package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.ActionModel;
import com.vmturbo.extractor.models.ActionModel.PendingAction;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Unit tests for the {@link ReportPendingActionWriter}.
 */
public class ReportPendingActionWriterTest {

    private static final long ACTION_WRITING_INTERVAL_MS = 10_000;

    private static final ActionSpec ACTION_SPEC = ActionSpec.newBuilder()
            .setExplanation("foo").build();

    private static final ActionOrchestratorAction ACTION = ActionOrchestratorAction.newBuilder()
            .setActionId(1121L)
            .setActionSpec(ACTION_SPEC)
            .build();

    private WriterConfig writerConfig = ImmutableWriterConfig.builder()
            .lastSeenUpdateIntervalMinutes(1)
            .lastSeenAdditionalFuzzMinutes(1)
            .insertTimeoutSeconds(10)
            .populateScopeTable(true)
            .build();

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private ActionConverter actionConverter = mock(ActionConverter.class);

    private DataProvider dataProvider = mock(DataProvider.class);

    private ExecutorService executorService = mock(ExecutorService.class);

    private ReportPendingActionWriter actionWriter;

    private List<Record> pendingActionReplaceCapture;
    private static final Logger logger = LogManager.getLogger();

    private final MultiStageTimer timer = new MultiStageTimer(logger);

    /**
     * Common setup code before each test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        final DbEndpoint endpoint = mock(DbEndpoint.class);
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslRecordSink entitiesReplacerSink = mock(DslReplaceRecordSink.class);
        this.pendingActionReplaceCapture = captureSink(entitiesReplacerSink, false);

        actionWriter = spy(new ReportPendingActionWriter(
                clock,
                executorService,
                endpoint,
                writerConfig,
                actionConverter,
                ACTION_WRITING_INTERVAL_MS));
        doReturn(entitiesReplacerSink).when(actionWriter).getPendingActionReplacerSink(
                any(DSLContext.class));
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
        final long actionId = 999;
        Record pendingActionRecord = new Record(ActionModel.PendingAction.TABLE);
        pendingActionRecord.set(PendingAction.ACTION_OID, actionId);

        when(actionConverter.makePendingActionRecord(ACTION_SPEC)).thenReturn(pendingActionRecord);

        // accept action and write
        actionWriter.recordAction(ACTION);
        actionWriter.write(new HashMap<>(), TypeInfoCase.MARKET, timer);

        assertThat(pendingActionReplaceCapture.get(0).asMap(), is(pendingActionRecord.asMap()));
    }
}
