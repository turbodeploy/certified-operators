package com.vmturbo.extractor.action;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
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
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

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
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk.Builder;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.RecordHashManager.SnapshotManager;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.topology.DataProvider;
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

    private ExecutorService pool = mock(ExecutorService.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private ActionHashManager actionHashManager = mock(ActionHashManager.class);

    private ActionsServiceMole actionsBackend = spy(ActionsServiceMole.class);

    private EntitySeverityService severityService = new EntitySeverityService();

    private ActionConverter actionConverter = mock(ActionConverter.class);

    private DataProvider dataProvider = mock(DataProvider.class);

    /**
     * Test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsBackend, severityService);

    private ReportingActionWriter reportingActionWriter = mock(ReportingActionWriter.class);
    private SearchActionWriter searchActionWriter = mock(SearchActionWriter.class);
    private Supplier<ReportingActionWriter> reportingActionWriterSupplier = mock(Supplier.class);
    private Supplier<SearchActionWriter> searchActionWriterSupplier = mock(Supplier.class);

    private ActionWriter actionWriter;

    /**
     * Common setup code before each test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        doReturn(new ReportingActionWriter(clock, pool, endpoint, writerConfig, actionConverter,
                actionHashManager, ACTION_WRITING_INTERVAL_MS)).when(reportingActionWriterSupplier).get();
        doReturn(new SearchActionWriter(dataProvider, endpoint, writerConfig, pool))
                .when(searchActionWriterSupplier).get();

        actionWriter = spy(new ActionWriter(clock,
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                EntitySeverityServiceGrpc.newStub(grpcServer.getChannel()),
                dataProvider,
                ACTION_WRITING_INTERVAL_MS,
                true,
                false,
                REALTIME_CONTEXT,
                reportingActionWriterSupplier,
                searchActionWriterSupplier));

        when(actionsBackend.getAllActions(any())).thenReturn(Collections.singletonList(
            FilteredActionResponse.newBuilder()
                .setActionChunk(ActionChunk.newBuilder()
                    .addActions(ActionOrchestratorAction.newBuilder()
                        .setActionSpec(ACTION)))
                .build()));
        severityService.setSeveritySupplier(Collections::emptyList);
        doAnswer(inv -> null).when(dataProvider).getTopologyGraph();
        when(actionHashManager.open(clock.millis())).thenReturn(mock(SnapshotManager.class));
    }

    /**
     * Test that plan contexts get ignored.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testIgnorePlanContext() throws Exception {
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT + 100));

        verify(actionWriter, never()).fetchActions(any());
        verify(actionWriter, never()).fetchSeverities(anyLong(), any());
    }

    /**
     * Test that the action writing interval is respected per context.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSkipUpdateForReporting() throws Exception {
        doAnswer(invocation -> {
            return null;
        }).when(actionWriter).fetchActions(any());
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(actionHashManager.open(anyLong())).thenReturn(snapshotManager);

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
        verify(actionWriter, times(1)).fetchActions(any());

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
        // Still written just once.
        verify(actionWriter, times(1)).fetchActions(any());

        // A buy RI plan should still get processed.
        actionWriter.onActionsUpdated(ActionsUpdated.newBuilder()
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                        .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                                .setTopologyContextId(REALTIME_CONTEXT)))
                .build());
        verify(actionWriter, times(2)).fetchActions(any());

        clock.addTime(ACTION_WRITING_INTERVAL_MS - 1, ChronoUnit.MILLIS);

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter, times(2)).fetchActions(any());

        clock.addTime(1, ChronoUnit.MILLIS);

        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter, times(3)).fetchActions(any());
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
        doThrow(new UnsupportedDialectException("bad!")).when(reportingActionWriter).write(any(), any(), any());
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
    }

    /**
     * Test that a SQL exception doesn't get propagated to the caller.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSQLException() throws Exception {
        doThrow(new SQLException("bad!", "SQL:FOO", 123)).when(reportingActionWriter).write(any(), any(), any());
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
    }

    /**
     * Test that disabling ingestion works.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDisableIngestion() throws Exception {
        ActionWriter actionWriter = spy(new ActionWriter(clock,
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                EntitySeverityServiceGrpc.newStub(grpcServer.getChannel()),
                dataProvider,
                ACTION_WRITING_INTERVAL_MS,
                false,
                false,
                REALTIME_CONTEXT,
                reportingActionWriterSupplier,
                searchActionWriterSupplier));
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter, never()).fetchActions(any());
        verify(actionWriter, never()).fetchSeverities(anyLong(), any());
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

    /**
     * Service implementation for retrieving entity severities.
     */
    private static class EntitySeverityService extends EntitySeverityServiceImplBase {

        private Supplier<List<EntitySeverity>> severitySupplier;

        @Override
        public void getEntitySeverities(MultiEntityRequest request,
                StreamObserver<EntitySeveritiesResponse> responseObserver) {
            Objects.requireNonNull(severitySupplier);
            Builder chunks = EntitySeveritiesChunk.newBuilder();
            severitySupplier.get().forEach(chunks::addEntitySeverity);
            responseObserver.onNext(EntitySeveritiesResponse.newBuilder().setEntitySeverity(chunks).build());
            responseObserver.onCompleted();
        }

        /**
         * This lambda should be given an implementation in any tests that call
         * actionOrchestratorImpl#getEntitySeverities.
         *
         * @param severitySupplier supplier of severity
         */
        public void setSeveritySupplier(Supplier<List<EntitySeverity>> severitySupplier) {
            this.severitySupplier = severitySupplier;
        }
    }
}
