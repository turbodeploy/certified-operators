package com.vmturbo.topology.processor.topology.pipeline;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceImplBase;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.actions.ActionConstraintsUploader;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsUploader;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingConfig;
import com.vmturbo.topology.processor.controllable.ControllableManager;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.GroupResolverSearchFilterResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.reservation.ReservationManager;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidator;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.topology.ApplicationCommodityKeyChanger;
import com.vmturbo.topology.processor.topology.CloudMigrationPlanHelper;
import com.vmturbo.topology.processor.topology.CommoditiesEditor;
import com.vmturbo.topology.processor.topology.DemandOverriddenCommodityEditor;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector;
import com.vmturbo.topology.processor.topology.EphemeralEntityEditor;
import com.vmturbo.topology.processor.topology.HistoricalEditor;
import com.vmturbo.topology.processor.topology.HistoryAggregator;
import com.vmturbo.topology.processor.topology.PlanTopologyScopeEditor;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor;
import com.vmturbo.topology.processor.topology.RequestAndLimitCommodityThresholdsInjector;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * Tests that pipeline stage ContextMembers are correct and that
 * all pipelines can be created without error.
 */
public class TopologyPipelineFactoryTest {

    final TopologyInfo planTopoInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(1)
        .setTopologyId(2)
        .setTopologyType(TopologyType.PLAN)
        .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanType("OPTIMIZE_CLOUD").build())
        .build();

    final TopologyInfo liveTopoInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(11)
        .setTopologyId(12)
        .setTopologyType(TopologyType.REALTIME)
        .build();

    private ReservationServiceStub reservationServiceStub;
    private GroupServiceBlockingStub groupServiceBlockingStub;

    /**
     * Setup.
     *
     * @throws Exception on exception.
     */
    @Before
    public void setup() throws Exception {
        GrpcTestServer grpcTestServerReservation = GrpcTestServer.newServer(new ReservationServiceImplBase() {
            @Override
            public void getAllReservations(GetAllReservationsRequest request, StreamObserver<Reservation> responseObserver) {
                super.getAllReservations(request, responseObserver);
            }
        });
        grpcTestServerReservation.start();
        reservationServiceStub =
            ReservationServiceGrpc.newStub(grpcTestServerReservation.getChannel());


        GrpcTestServer grpcTestServerGroup = GrpcTestServer.newServer(new GroupServiceImplBase() {
            @Override
            public void createGroup(CreateGroupRequest request, StreamObserver<CreateGroupResponse> responseObserver) {
                super.createGroup(request, responseObserver);
            }
        });
        grpcTestServerGroup.start();
        groupServiceBlockingStub = GroupServiceGrpc.newBlockingStub(grpcTestServerGroup.getChannel());
    }

    /**
     * testLivePipeline.
     */
    @Test
    public void testLivePipeline() {
        final MatrixInterface matrixInterface = mock(MatrixInterface.class);
        when(matrixInterface.copy()).thenReturn(matrixInterface);
        final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);
        when(licenseCheckClient.isDevFreemium()).thenReturn(false);

        final LivePipelineFactory lpf = livePipelineFactory(matrixInterface, licenseCheckClient);
        lpf.liveTopology(liveTopoInfo, Collections.emptyList(), mock(StitchingJournalFactory.class));
    }

    /**
     * testFreemiumPipeline.
     */
    @Test
    public void testFreemiumPipeline() {
        final MatrixInterface matrixInterface = mock(MatrixInterface.class);
        when(matrixInterface.copy()).thenReturn(matrixInterface);
        final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);
        when(licenseCheckClient.isDevFreemium()).thenReturn(false);

        final LivePipelineFactory lpf = livePipelineFactory(matrixInterface, licenseCheckClient);
        lpf.liveTopology(liveTopoInfo, Collections.emptyList(), mock(StitchingJournalFactory.class));
    }

    /**
     * testPlanOverLivePipeline.
     */
    @Test
    public void testPlanOverLivePipeline() {
        final PlanPipelineFactory ppf = planPipelineFactory();
        ppf.planOverLiveTopology(planTopoInfo, Collections.emptyList(),
            PlanScope.getDefaultInstance(), mock(StitchingJournalFactory.class));
    }

    /**
     * testPlanOverPlanPipeline.
     */
    @Test
    public void testPlanOverPlanPipeline() {
        final PlanPipelineFactory ppf = planPipelineFactory();
        ppf.planOverOldTopology(planTopoInfo, Collections.emptyList(), PlanScope.getDefaultInstance());
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private LivePipelineFactory livePipelineFactory(@Nonnull final MatrixInterface matrixInterface,
                                                    @Nonnull final LicenseCheckClient licenseCheckClient) {
        return new LivePipelineFactory(
            mock(TopoBroadcastManager.class),
            mock(PolicyManager.class),
            mock(StitchingManager.class),
            mock(DiscoveredTemplateDeploymentProfileNotifier.class),
            mock(DiscoveredGroupUploader.class),
            mock(DiscoveredWorkflowUploader.class),
            mock(DiscoveredCloudCostUploader.class),
            mock(EntitySettingsResolver.class),
            mock(EntitySettingsApplicator.class),
            mock(EnvironmentTypeInjector.class),
            mock(SearchResolver.class),
            groupServiceBlockingStub,
            mock(ReservationManager.class),
            mock(DiscoveredSettingPolicyScanner.class),
            mock(EntityValidator.class),
            mock(SupplyChainValidator.class),
            mock(DiscoveredClusterConstraintCache.class),
            mock(ApplicationCommodityKeyChanger.class),
            mock(ControllableManager.class),
            mock(HistoricalEditor.class),
            matrixInterface,
            mock(CachedTopology.class),
            mock(ProbeActionCapabilitiesApplicatorEditor.class),
            mock(HistoryAggregator.class),
            licenseCheckClient,
            mock(ConsistentScalingConfig.class),
            mock(ActionConstraintsUploader.class),
            mock(ActionMergeSpecsUploader.class),
            mock(RequestAndLimitCommodityThresholdsInjector.class),
            mock(EphemeralEntityEditor.class),
            reservationServiceStub,
            mock(GroupResolverSearchFilterResolver.class),
            mock(GroupScopeResolver.class),
            10);
    }

    private PlanPipelineFactory planPipelineFactory() {
        @SuppressWarnings("unchecked")
        final PlanPipelineFactory ppf = new PlanPipelineFactory(
            mock(TopoBroadcastManager.class),
            mock(PolicyManager.class),
            mock(StitchingManager.class),
            mock(EntitySettingsResolver.class),
            mock(EntitySettingsApplicator.class),
            mock(EnvironmentTypeInjector.class),
            mock(TopologyEditor.class),
            mock(RepositoryClient.class),
            mock(SearchResolver.class),
            groupServiceBlockingStub,
            mock(ReservationManager.class),
            mock(EntityValidator.class),
            mock(DiscoveredClusterConstraintCache.class),
            mock(ApplicationCommodityKeyChanger.class),
            mock(CommoditiesEditor.class),
            mock(PlanTopologyScopeEditor.class),
            mock(ProbeActionCapabilitiesApplicatorEditor.class),
            mock(HistoricalEditor.class),
            mock(MatrixInterface.class),
            mock(CachedTopology.class),
            mock(HistoryAggregator.class),
            mock(DemandOverriddenCommodityEditor.class),
            mock(ConsistentScalingConfig.class),
            mock(RequestAndLimitCommodityThresholdsInjector.class),
            mock(EphemeralEntityEditor.class),
            mock(GroupResolverSearchFilterResolver.class),
            mock(CloudMigrationPlanHelper.class));

        return ppf;
    }
}
