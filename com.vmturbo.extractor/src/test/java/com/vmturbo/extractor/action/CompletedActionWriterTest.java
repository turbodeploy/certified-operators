package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.action.CompletedActionWriter.RecordBatchWriter;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.models.ActionModel.CompletedAction;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for the {@link CompletedActionWriter}.
 */
public class CompletedActionWriterTest {

    private CompletedActionWriter writer;

    private RecordBatchWriter recordBatchWriter;

    private List<Record> executedActionUpsertCapture;

    private ActionConverter actionConverter = mock(ActionConverter.class);

    private DataProvider dataProvider = mock(DataProvider.class);

    private TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);

    private ExtractorFeatureFlags featureFlags = mock(ExtractorFeatureFlags.class);

    private final DbEndpoint endpoint = mock(DbEndpoint.class);

    private final ExtractorKafkaSender extractorKafkaSender = mock(ExtractorKafkaSender.class);

    private DataExtractionFactory dataExtractionFactory = mock(DataExtractionFactory.class);

    private ActionWriterFactory actionWriterFactory = mock(ActionWriterFactory.class);

    private Clock clock = mock(Clock.class);

    private RelatedEntitiesExtractor relatedEntitiesExtractor = mock(RelatedEntitiesExtractor.class);

    private List<ExportedObject> exportedActionsCapture;

    /**
     * Common code to run before every test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslUpsertRecordSink upsertSink = mock(DslUpsertRecordSink.class);
        this.executedActionUpsertCapture = captureSink(upsertSink, false);

        when(featureFlags.isReportingEnabled()).thenReturn(true);
        when(featureFlags.isExtractionEnabled()).thenReturn(true);

        // capture actions sent to kafka
        this.exportedActionsCapture = new ArrayList<>();
        doAnswer(inv -> {
            Collection<ExportedObject> exportedObjects = inv.getArgumentAt(0, Collection.class);
            if (exportedObjects != null) {
                exportedActionsCapture.addAll(exportedObjects);
            }
            return null;
        }).when(extractorKafkaSender).send(any());

        ExecutorService batchExecutor = mock(ExecutorService.class);
        writer = new CompletedActionWriter(endpoint, batchExecutor, actionConverter, dataProvider,
                featureFlags, dsl -> upsertSink, extractorKafkaSender,
                dataExtractionFactory, actionWriterFactory, clock);
        ArgumentCaptor<Runnable> submittedBatchWriter = ArgumentCaptor.forClass(Runnable.class);
        verify(batchExecutor).submit(submittedBatchWriter.capture());
        recordBatchWriter = (RecordBatchWriter)submittedBatchWriter.getValue();

        when(dataProvider.getTopologyGraph()).thenReturn(topologyGraph);
        when(dataExtractionFactory.newRelatedEntitiesExtractor(dataProvider)).thenReturn(Optional.of(relatedEntitiesExtractor));
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

        assertThat(exportedActionsCapture.size(), is(1));
        assertThat(exportedActionsCapture.get(0).getAction().getOid(), is(1L));
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

        assertThat(exportedActionsCapture.size(), is(1));
        assertThat(exportedActionsCapture.get(0).getAction().getOid(), is(1L));
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

        assertThat(exportedActionsCapture.size(), is(2));
        assertThat(exportedActionsCapture.stream().map(ExportedObject::getAction)
                .map(Action::getOid).collect(Collectors.toList()), containsInAnyOrder(1L, 2L));
    }


    Pair<ActionSpec, Record> fakeAction(final long id, String finalMessage) {
        ActionSpec spec = ActionSpec.newBuilder()
                .setExplanation(Long.toString(id))
                .build();
        Record mappedRecord = new Record(CompletedAction.TABLE);
        mappedRecord.set(CompletedAction.ACTION_OID, spec.getRecommendation().getId());
        mappedRecord.set(CompletedAction.FINAL_MESSAGE, finalMessage);
        when(actionConverter.makeExecutedActionSpec(eq(spec), eq(finalMessage), eq(topologyGraph)))
            .thenReturn(mappedRecord);

        Action action = new Action();
        action.setOid(id);
        when(actionConverter.makeExportedAction(eq(spec), eq(topologyGraph), eq(new HashMap<>()),
                eq(Optional.of(relatedEntitiesExtractor)))).thenReturn(action);
        return Pair.of(spec, mappedRecord);
    }

    /**
     * Test that disabling reporting prevents completed actions from being recorded.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testReportingDisabled() throws Exception {
        when(featureFlags.isReportingEnabled()).thenReturn(false);
        when(featureFlags.isExtractionEnabled()).thenReturn(false);

        writer.onActionSuccess(ActionSuccess.newBuilder()
                .setActionId(1)
                .setSuccessDescription("SUCCESS 1")
                .setActionSpec(ActionSpec.newBuilder()
                     .setExplanation("foo"))
                .build());

        recordBatchWriter.runIteration();

        // Nothing converted.
        verifyZeroInteractions(actionConverter);
        // No interactions with the endpoint, because there are no reporting actions to ingest.
        verifyZeroInteractions(endpoint);
    }
}