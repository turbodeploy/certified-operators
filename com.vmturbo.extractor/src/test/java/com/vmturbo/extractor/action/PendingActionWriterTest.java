package com.vmturbo.extractor.action;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.mutable.MutableLong;
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
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
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
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.ImmutableWriterConfig;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for the {@link PendingActionWriter}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class PendingActionWriterTest {

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
            .populateScopeTable(true)
            .build();

    private ExecutorService pool = mock(ExecutorService.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private ActionsServiceMole actionsBackend = spy(ActionsServiceMole.class);

    private EntitySeverityService severityService = new EntitySeverityService();

    private PolicyServiceMole policyBackend = spy(PolicyServiceMole.class);

    private ActionConverter actionConverter = mock(ActionConverter.class);

    private DataProvider dataProvider = mock(DataProvider.class);

    /**
     * Test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsBackend, severityService, policyBackend);

    private ReportPendingActionWriter reportingActionWriter = mock(ReportPendingActionWriter.class);
    private ActionWriterFactory actionWriterFactory = mock(ActionWriterFactory.class);
    private ExtractorKafkaSender extractorKafkaSender = mock(ExtractorKafkaSender.class);
    private DataExtractionFactory dataExtractionFactory = mock(DataExtractionFactory.class);
    private ActionAttributeExtractor actionAttributeExtractor = mock(ActionAttributeExtractor.class);
    private TopologyGraph<SupplyChainEntity> topologyGraph = mock(TopologyGraph.class);
    private MutableLong lastWrite = new MutableLong(0);

    private ExtractorFeatureFlags extractorFeatureFlags = mock(ExtractorFeatureFlags.class);

    private PendingActionWriter actionWriter;

    /**
     * Common setup code before each test.
     *
     * @throws Exception To satisfy compiler.
     */
    @Before
    public void setup() throws Exception {
        when(dataProvider.getTopologyGraph()).thenReturn(topologyGraph);
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        doReturn(Optional.of(new ReportPendingActionWriter(clock, pool, endpoint, writerConfig,
                actionConverter, topologyGraph, ACTION_WRITING_INTERVAL_MS, TypeInfoCase.MARKET, new HashMap<>())))
                .when(actionWriterFactory).getReportPendingActionWriter(any());
        doReturn(Optional.of(new DataExtractionPendingActionWriter(extractorKafkaSender,
                dataExtractionFactory, dataProvider, clock, lastWrite, actionAttributeExtractor)))
                .when(actionWriterFactory).getDataExtractionActionWriter();

        when(extractorFeatureFlags.isReportingActionIngestionEnabled()).thenReturn(true);

        actionWriter = spy(new PendingActionWriter(
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                EntitySeverityServiceGrpc.newStub(grpcServer.getChannel()),
                PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                dataProvider,
                extractorFeatureFlags,
                REALTIME_CONTEXT,
                actionWriterFactory));

        when(actionsBackend.getAllActions(any())).thenReturn(Collections.singletonList(
                FilteredActionResponse.newBuilder()
                        .setActionChunk(ActionChunk.newBuilder()
                                .addActions(ActionOrchestratorAction.newBuilder()
                                        .setActionSpec(ACTION)))
                        .build()));
        severityService.setSeveritySupplier(Collections::emptyList);
        doReturn(Collections.emptyList()).when(policyBackend).getPolicies(any());
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
     * Test that an AO exception doesn't get propagated to the caller.
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
        doThrow(new UnsupportedDialectException("bad!")).when(reportingActionWriter).write(any());
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
    }

    /**
     * Test that a SQL exception doesn't get propagated to the caller.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSQLException() throws Exception {
        doThrow(new SQLException("bad!", "SQL:FOO", 123)).when(reportingActionWriter).write(any());
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));
    }

    /**
     * Test that disabling ingestion works.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDisableIngestion() throws Exception {
        when(extractorFeatureFlags.isReportingActionIngestionEnabled()).thenReturn(false);
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter, never()).fetchPolicies(any());
        verify(actionWriter, never()).fetchActions(any());
        verify(actionWriter, never()).fetchSeverities(anyLong(), any());
    }

    /**
     * Test that policies are fetched when data extraction is enabled.
     */
    @Test
    public void testFetchPolicies() {
        when(extractorFeatureFlags.isReportingActionIngestionEnabled()).thenReturn(false);
        when(extractorFeatureFlags.isExtractionEnabled()).thenReturn(true);
        when(actionsBackend.getAllActions(any())).thenReturn(Collections.emptyList());
        actionWriter.onActionsUpdated(actionsUpdated(REALTIME_CONTEXT));

        verify(actionWriter).fetchPolicies(any());
        verify(actionWriter).fetchActions(any());
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