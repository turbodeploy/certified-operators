package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.extractor.action.CompletedActionWriter.RecordBatchWriter;
import com.vmturbo.extractor.models.ActionModel.CompletedAction;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Unit tests for the {@link CompletedActionWriter}.
 */
public class CompletedPendingActionWriterTest {

    private CompletedActionWriter writer;

    private RecordBatchWriter recordBatchWriter;

    private List<Record> executedActionUpsertCapture;

    private ActionConverter actionConverter = mock(ActionConverter.class);

    /**
     * Common code to run before every test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        ExecutorService batchExecutor = mock(ExecutorService.class);
        final DbEndpoint endpoint = mock(DbEndpoint.class);
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslUpsertRecordSink upsertSink = mock(DslUpsertRecordSink.class);
        this.executedActionUpsertCapture = captureSink(upsertSink, false);

        writer = new CompletedActionWriter(endpoint, batchExecutor, actionConverter, dsl -> upsertSink);
        ArgumentCaptor<Runnable> submittedBatchWriter = ArgumentCaptor.forClass(Runnable.class);
        verify(batchExecutor).submit(submittedBatchWriter.capture());
        recordBatchWriter = (RecordBatchWriter)submittedBatchWriter.getValue();
    }

    /**
     * Test that a succeeded action is inserted into the {@link DslUpsertRecordSink}.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSucceededAction() throws Exception {
        Pair<ActionSpec, Record> action = fakeAction(1, "SUCCESS");
        writer.onActionSuccess(ActionSuccess.newBuilder()
                .setActionId(1)
                .setSuccessDescription("SUCCESS")
                .setActionSpec(action.getKey())
                .build());

        recordBatchWriter.runIteration();


        assertThat(executedActionUpsertCapture.size(), is(1));
        assertThat(executedActionUpsertCapture.get(0).asMap(), is(action.getValue().asMap()));
    }

    /**
     * Test that a failed action is inserted into the {@link DslUpsertRecordSink}.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testFailedAction() throws Exception {
        Pair<ActionSpec, Record> action = fakeAction(1, "FAILED");
        writer.onActionFailure(ActionFailure.newBuilder()
                .setActionId(1)
                .setErrorDescription("FAILED")
                .setActionSpec(action.getKey())
                .build());

        recordBatchWriter.runIteration();


        assertThat(executedActionUpsertCapture.size(), is(1));
        assertThat(executedActionUpsertCapture.get(0).asMap(), is(action.getValue().asMap()));
    }

    /**
     * Test that a single iteration of the {@link RecordBatchWriter} records all currently queued
     * actions.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testActionBatch() throws Exception {
        Pair<ActionSpec, Record> action1 = fakeAction(1, "SUCCESS 1");
        Pair<ActionSpec, Record> action2 = fakeAction(2, "SUCCESS 2");

        writer.onActionSuccess(ActionSuccess.newBuilder()
                .setActionId(1)
                .setSuccessDescription("SUCCESS 1")
                .setActionSpec(action1.getKey())
                .build());
        writer.onActionSuccess(ActionSuccess.newBuilder()
                .setActionId(2)
                .setSuccessDescription("SUCCESS 2")
                .setActionSpec(action2.getKey())
                .build());

        recordBatchWriter.runIteration();

        assertThat(executedActionUpsertCapture.size(), is(2));
        assertThat(executedActionUpsertCapture.get(0).asMap(), is(action1.getValue().asMap()));
        assertThat(executedActionUpsertCapture.get(1).asMap(), is(action2.getValue().asMap()));
    }


    Pair<ActionSpec, Record> fakeAction(final long id, String finalMessage) {
        ActionSpec spec = ActionSpec.newBuilder()
                .setExplanation(Long.toString(id))
                .build();
        Record mappedRecord = new Record(CompletedAction.TABLE);
        mappedRecord.set(CompletedAction.ACTION_OID, spec.getRecommendation().getId());
        mappedRecord.set(CompletedAction.FINAL_MESSAGE, finalMessage);
        when(actionConverter.makeExecutedActionSpec(spec, finalMessage))
                .thenReturn(mappedRecord);
        return Pair.of(spec, mappedRecord);
    }
}