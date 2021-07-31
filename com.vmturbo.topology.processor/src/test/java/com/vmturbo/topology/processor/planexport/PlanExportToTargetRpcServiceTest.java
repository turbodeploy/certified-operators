package com.vmturbo.topology.processor.planexport;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.stub.StreamObserver;

import jersey.repackaged.com.google.common.collect.Sets;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse.ActionChunk;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.common.protobuf.topology.PlanExport.PlanExportToTargetRequest;
import com.vmturbo.common.protobuf.topology.PlanExport.PlanExportToTargetResponse;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;
import com.vmturbo.topology.processor.api.PlanExportNotificationListener;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.planexport.PlanExport;

/**
 * Unit test for {@link PlanExportToTargetRpcService}.
 */
@RunWith(MockitoJUnitRunner.class)
public class PlanExportToTargetRpcServiceTest {
    /**
     * PlanExportRpcService object.
     */
    private final TestActionsService actionsRpcService = spy(TestActionsService.class);

    private static final long DESTINATION_OID = 481516L;
    private static final long TARGET_ID = 2342L;

    @Captor
    ArgumentCaptor<List<Action>> actionsCaptor;

    /**
     * The grpc server.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionsRpcService);

    /**
     * Test the exportPlan RPC call implementation.
     *
     * @throws Exception if there is an error
     */
    @Test
    public void testExportRpcCall() throws Exception {
        final StreamObserver<PlanExportToTargetResponse> mockObserver = mock(StreamObserver.class);

        NonMarketEntityDTO pdDTO = NonMarketEntityDTO.newBuilder()
            .setEntityType(NonMarketEntityType.PLAN_DESTINATION)
            .setId("required")
            .setDisplayName("required")
            .build();

        PlanExportDTO exportData = PlanExportDTO.newBuilder()
            .setPlanName("Name")
            .setMarketId("Market")
            .build();

        final PlanExportHelper helper = mock(PlanExportHelper.class);
        when(helper.buildPlanExportDTO(any(), any())).thenReturn(exportData);
        when(helper.buildPlanDestinationNonMarketEntityDTO(any())).thenReturn(pdDTO);

        final OperationManager operationManager = mock(OperationManager.class);
        when(operationManager.exportPlan(any(), any(), anyLong(), anyLong()))
            .thenReturn(mock(PlanExport.class));

        ActionsServiceBlockingStub actionServiceStub =
            ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        final PlanExportNotificationListener listener = mock(PlanExportNotificationListener.class);
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("plan-export-starter-%d")
            .build();
        final ExecutorService exportExecutor = Executors.newFixedThreadPool(1, threadFactory);

        final PlanExportToTargetRpcService exportRpcService =
            new PlanExportToTargetRpcService(operationManager, listener, helper,
                actionServiceStub, exportExecutor, null);

        final PlanExportToTargetRequest request = PlanExportToTargetRequest.newBuilder()
            .setPlan(PlanInstance.newBuilder()
                .setPlanId(42)
                .setStatus(PlanStatus.SUCCEEDED)
                .build())
            .setDestination(PlanDestination.newBuilder()
                .setOid(DESTINATION_OID)
                .setTargetId(TARGET_ID)
                .build())
            .build();

        exportRpcService.exportPlan(request, mockObserver);

        // Verify
        final PlanExportToTargetResponse expectedResponse = PlanExportToTargetResponse.newBuilder()
            .setStatus(PlanExportStatus.newBuilder()
                .setState(PlanExportState.IN_PROGRESS)
                .setDescription(PlanExportToTargetRpcService.STARTING_EXPORT_MESSAGE)
                .setProgress(0).build())
            .build();

        verify(mockObserver, timeout(10000)).onNext(any(PlanExportToTargetResponse.class));
        verify(mockObserver, times(1)).onNext(expectedResponse);
        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);

        verify(actionsRpcService, timeout(10000)).getAllActions(any());

        verify(helper, timeout(10000)).buildPlanExportDTO(any(), actionsCaptor.capture());
        Set<Long> actionIds = actionsCaptor.getValue().stream().map(Action::getId)
            .collect(Collectors.toSet());

        // Verify that the actions are the ones we got from the actions service.
        // We don't need to verify any of the details of the conversion as that is
        // handled in PlanExportHelper tests.
        assertEquals(Sets.newHashSet(1L, 2L, 3L, 4L), actionIds);

        verify(operationManager, timeout(10000)).exportPlan(eq(exportData), eq(pdDTO),
            eq(DESTINATION_OID), eq(TARGET_ID));
    }

    /**
     * Returns actions with fixed IDs 1,2,3,4.
     */
    private static class TestActionsService extends ActionsServiceMole {
        TestActionsService() {
        }

        @Override
        public List<FilteredActionResponse> getAllActions(FilteredActionRequest input) {
            FilteredActionResponse response12 = FilteredActionResponse.newBuilder()
                .setActionChunk(ActionChunk.newBuilder()
                    .addActions(aoActionWithId(1L))
                    .addActions(aoActionWithId(2L))
                ).build();

            FilteredActionResponse response34 = FilteredActionResponse.newBuilder()
                .setActionChunk(ActionChunk.newBuilder()
                    .addActions(aoActionWithId(3L))
                    .addActions(aoActionWithId(4L))
                ).build();

            return Arrays.asList(response12, response34);
        }
    }

    private static ActionOrchestratorAction aoActionWithId(long id) {
        return ActionOrchestratorAction.newBuilder()
            .setActionId(id)
            .setActionSpec(ActionSpec.newBuilder()
                .setRecommendation(Action.newBuilder()
                    .setId(id)
                    .setExplanation(Explanation.getDefaultInstance())
                    .setInfo(ActionInfo.getDefaultInstance())
                    .setDeprecatedImportance(12345.6789)
                )
            )
            .build();
    }
}
