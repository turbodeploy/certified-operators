package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
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

        when(featureFlags.isReportingActionIngestionEnabled()).thenReturn(true);
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
                featureFlags, dsl -> upsertSink, extractorKafkaSender, clock);
        ArgumentCaptor<Runnable> submittedBatchWriter = ArgumentCaptor.forClass(Runnable.class);
        verify(batchExecutor).submit(submittedBatchWriter.capture());
        recordBatchWriter = (RecordBatchWriter)submittedBatchWriter.getValue();

        when(dataProvider.getTopologyGraph()).thenReturn(topologyGraph);
        when(dataExtractionFactory.newRelatedEntitiesExtractor()).thenReturn(Optional.of(relatedEntitiesExtractor));
    }

    /**
     * Test that a succeeded action is inserted into the {@link DslUpsertRecordSink}.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSucceededAction() throws Exception {
        ExecutedAction action = fakeAction(1, "SUCCESS");
        Map<ExecutedAction, Pair<Record, Action>> expectedMappings = setupMockedConversion(action);
        writer.onActionSuccess(ActionSuccess.newBuilder()
                .setActionId(1)
                .setSuccessDescription("SUCCESS")
                .setActionSpec(action.getActionSpec())
                .build());

        recordBatchWriter.runIteration();

        validateCapturedResults(expectedMappings);
    }

    /**
     * Test that a failed action is inserted into the {@link DslUpsertRecordSink}.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testFailedAction() throws Exception {
        ExecutedAction action = fakeAction(1, "FAILED");
        Map<ExecutedAction, Pair<Record, Action>> expectedMappings = setupMockedConversion(action);
        writer.onActionFailure(ActionFailure.newBuilder()
                .setActionId(1)
                .setErrorDescription("FAILED")
                .setActionSpec(action.getActionSpec())
                .build());

        recordBatchWriter.runIteration();

        validateCapturedResults(expectedMappings);
    }

    /**
     * Test that a single iteration of the {@link RecordBatchWriter} records all currently queued
     * actions.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testActionBatch() throws Exception {
        final ExecutedAction action1 = fakeAction(1, "SUCCESS 1");
        final ExecutedAction action2 = fakeAction(2, "SUCCESS 2");
        final Map<ExecutedAction, Pair<Record, Action>> expectedMappings = setupMockedConversion(action1, action2);

        writer.onActionSuccess(ActionSuccess.newBuilder()
                .setActionId(1)
                .setSuccessDescription("SUCCESS 1")
                .setActionSpec(action1.getActionSpec())
                .build());
        writer.onActionSuccess(ActionSuccess.newBuilder()
                .setActionId(2)
                .setSuccessDescription("SUCCESS 2")
                .setActionSpec(action2.getActionSpec())
                .build());

        recordBatchWriter.runIteration();

        validateCapturedResults(expectedMappings);
    }


    private void validateCapturedResults(Map<ExecutedAction, Pair<Record, Action>> expectedMappings) {
        final Map<Long, Record> capturedRecords = executedActionUpsertCapture.stream()
                .collect(Collectors.toMap(r -> r.get(CompletedAction.ACTION_OID), Function.identity()));
        final Map<Long, Action> capturedActions = exportedActionsCapture.stream()
                .map(ExportedObject::getAction)
                .collect(Collectors.toMap(a -> a.getOid(), Function.identity()));

        assertThat(capturedRecords.size(), is(expectedMappings.size()));
        assertThat(capturedActions.size(), is(expectedMappings.size()));
        expectedMappings.forEach((expectedAction, expectedResults) -> {
            assertThat(capturedRecords.get(expectedAction.getActionId()).asMap(), is(expectedResults.getLeft().asMap()));
            assertThat(capturedActions.get(expectedAction.getActionId()), is(expectedResults.getRight()));
        });
    }

    private Map<ExecutedAction, Pair<Record, Action>> setupMockedConversion(ExecutedAction... actions) {
        List<Record> bulkRecordResponse = new ArrayList<>();
        List<Action> bulkExtractionResponse = new ArrayList<>();
        Map<ExecutedAction, Pair<Record, Action>> retMap = new HashMap<>();
        for (ExecutedAction action : actions) {
            Record mappedRecord = new Record(CompletedAction.TABLE);
            mappedRecord.set(CompletedAction.ACTION_OID, action.getActionId());
            mappedRecord.set(CompletedAction.FINAL_MESSAGE, action.getMessage());
            bulkRecordResponse.add(mappedRecord);

            Action exportedAction = new Action();
            exportedAction.setOid(action.getActionId());
            bulkExtractionResponse.add(exportedAction);

            retMap.put(action, Pair.of(mappedRecord, exportedAction));
        }

        when(actionConverter.makeExecutedActionSpec(any())).thenReturn(bulkRecordResponse);
        when(actionConverter.makeExportedActions(any())).thenReturn(bulkExtractionResponse);
        return retMap;
    }

    private ExecutedAction fakeAction(final long id, String finalMessage) {
        ActionSpec spec = ActionSpec.newBuilder()
                .setExplanation(Long.toString(id))
                .build();
        return new ExecutedAction(id, spec, finalMessage);
    }

    /**
     * Test that disabling reporting prevents completed actions from being recorded.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testReportingDisabled() throws Exception {
        when(featureFlags.isReportingEnabled()).thenReturn(false);
        when(featureFlags.isReportingActionIngestionEnabled()).thenReturn(false);
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