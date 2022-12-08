package com.vmturbo.topology.processor.topology.pipeline;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

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
import com.vmturbo.components.common.pipeline.Pipeline;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule.FeatureFlagTest;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.actions.ActionConstraintsUploader;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsUploader;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingConfig;
import com.vmturbo.topology.processor.controllable.ControllableManager;
import com.vmturbo.topology.processor.cost.AliasedOidsUploader;
import com.vmturbo.topology.processor.cost.BilledCloudCostUploader;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.entity.EntityCustomTagsMerger;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.GroupResolverSearchFilterResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.listeners.HistoryVolumesListener;
import com.vmturbo.topology.processor.listeners.TpAppSvcHistoryListener;
import com.vmturbo.topology.processor.planexport.DiscoveredPlanDestinationUploader;
import com.vmturbo.topology.processor.reservation.ReservationManager;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidator;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.TargetStore;
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
import com.vmturbo.topology.processor.topology.PostScopingTopologyEditor;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor;
import com.vmturbo.topology.processor.topology.RequestAndLimitCommodityThresholdsInjector;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
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

    private MatrixInterface matrixInterface;
    private LicenseCheckClient licenseCheckClient;
    private ReservationServiceStub reservationServiceStub;
    private GroupServiceBlockingStub groupServiceBlockingStub;
    private StitchingManager stitchingManager;
    private EntityStore entityStore;
    private StalenessInformationProvider stalenessInformationProvider;
    private StitchingJournalFactory stitchingJournalFactory;
    private EnvironmentTypeInjector environmentTypeInjector;
    private PolicyManager policyManager;
    private HistoryVolumesListener historyVolumesListener;
    private TpAppSvcHistoryListener appSvcHistoryListener;
    private EntitySettingsResolver entitySettingsResolver;
    private TopoBroadcastManager topoBroadcastManager;
    private StitchingContext stitchingContext;
    private ReservationManager reservationManager;
    private EphemeralEntityEditor ephemeralEntityEditor;
    private ProbeActionCapabilitiesApplicatorEditor probeActionCapabilitiesApplicatorEditor;
    private PipelineInput pipelineInput;

    /**
     * Rule to initialize FeatureFlags store.
     **/
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

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
        stalenessInformationProvider = mock(StalenessInformationProvider.class);
        groupServiceBlockingStub = GroupServiceGrpc.newBlockingStub(grpcTestServerGroup.getChannel());
        mockStitchingContext();
        mockStitchingManager();
        mockEntityStore();
        mockPipelineInput();
        mockEnvironmentInjector();
        mockPolicyManager();
        mockHistoryVolumesListener();
        mockTpAppSvcDaysEmptyListener();
        mockEntitySettingsResolver();
        mockBroadcastManager();
        mockReservationManager();
        mockEphemeralEntityEditor();
        mockProbeActionCapabilitiesApplicatorEditor();
        mockMatrixInterface();
        mockLicenseCheckClient();
    }

    private void mockStitchingContext() {
        final IdentityProvider identityProvider = mock(IdentityProvider.class);
        final TargetStore targetStore = mock(TargetStore.class);
        stitchingContext = StitchingContext.newBuilder(5, targetStore).setIdentityProvider(identityProvider).build();
    }

    private void mockStitchingManager() {
        final StitchingJournal<StitchingEntity> stitchingJournal = new StitchingJournal<>();
        stitchingJournalFactory = mock(StitchingJournalFactory.class);
        when(stitchingJournalFactory.stitchingJournal(any(StitchingContext.class))).thenReturn(stitchingJournal);
        stitchingManager = mock(StitchingManager.class);
        when(stitchingManager.stitch(any(), any())).thenReturn(stitchingContext);
    }

    private void mockEntityStore() {
        entityStore = mock(EntityStore.class);
        when(entityStore.constructStitchingContext(stalenessInformationProvider))
            .thenReturn(stitchingContext);
    }

    private void mockPipelineInput() {
        pipelineInput = mock(PipelineInput.class);
        when(pipelineInput.getEntityStore()).thenReturn(entityStore);
        when(pipelineInput.getStitchingContext()).thenReturn(stitchingContext);
    }

    private void mockEnvironmentInjector() {
        environmentTypeInjector = mock(EnvironmentTypeInjector.class);
        when(environmentTypeInjector.injectEnvironmentType(any()))
            .thenReturn(new EnvironmentTypeInjector.InjectionSummary(0, 0, new HashMap<>()));
    }

    private void mockPolicyManager() {
        policyManager = mock(PolicyManager.class);
        when(policyManager.applyPolicies(any(), any(), any(), any(), any()))
            .thenReturn(new PolicyApplicator.Results());
    }

    private void mockHistoryVolumesListener() {
        historyVolumesListener = mock(HistoryVolumesListener.class);
        when(historyVolumesListener.getVolIdToLastAttachmentTime()).thenReturn(new HashMap<>());
    }

    private void mockTpAppSvcDaysEmptyListener() {
        appSvcHistoryListener = mock(TpAppSvcHistoryListener.class);
        when(appSvcHistoryListener.getDaysEmptyInfosByAppSvc()).thenReturn(new HashMap<>());
    }

    private void mockEntitySettingsResolver() {
        entitySettingsResolver = mock(EntitySettingsResolver.class);
        final TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);
        when(topologyGraph.topSort()).thenReturn(Stream.of());
        when(topologyGraph.entities()).thenReturn(Stream.of()).thenReturn(Stream.of());
        when(entitySettingsResolver.resolveSettings(any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(new GraphWithSettings(topologyGraph, new HashMap<>(), new HashMap<>()));
    }

    private void mockBroadcastManager() {
        topoBroadcastManager = mock(TopoBroadcastManager.class);
        final TopologyBroadcast topologyBroadcast = mock(TopologyBroadcast.class);
        when(topoBroadcastManager.broadcastLiveTopology(any())).thenReturn(topologyBroadcast);
    }

    private void mockReservationManager() {
        reservationManager = mock(ReservationManager.class);
        when(reservationManager.applyReservation(any())).thenReturn(Pipeline.Status.success());
    }

    private void mockEphemeralEntityEditor() {
        ephemeralEntityEditor = mock(EphemeralEntityEditor.class);
        when(ephemeralEntityEditor.applyEdits(any())).thenReturn(new EphemeralEntityEditor.EditSummary());
    }

    private void mockProbeActionCapabilitiesApplicatorEditor() {
        probeActionCapabilitiesApplicatorEditor = mock(ProbeActionCapabilitiesApplicatorEditor.class);
        when(probeActionCapabilitiesApplicatorEditor.applyPropertiesEdits(any()))
            .thenReturn(new ProbeActionCapabilitiesApplicatorEditor.EditorSummary());
    }

    private void mockMatrixInterface() {
        matrixInterface = mock(MatrixInterface.class);
        when(matrixInterface.copy()).thenReturn(matrixInterface);
    }

    private void mockLicenseCheckClient() {
        licenseCheckClient = mock(LicenseCheckClient.class);
        when(licenseCheckClient.isDevFreemium()).thenReturn(false);
    }

    /**
     * testLivePipeline.
     */
    @Test
    public void testLivePipeline() {
        final LivePipelineFactory lpf = livePipelineFactory(matrixInterface, licenseCheckClient);
        lpf.liveTopology(liveTopoInfo, Collections.emptyList(), mock(StitchingJournalFactory.class));
    }

    /**
     * testFreemiumPipeline.
     */
    @Test
    public void testFreemiumPipeline() {
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
            PlanScope.getDefaultInstance(), null, mock(StitchingJournalFactory.class));
    }

    /**
     * testPlanOverPlanPipeline.
     */
    @Test
    public void testPlanOverPlanPipeline() {
        final PlanPipelineFactory ppf = planPipelineFactory();
        ppf.planOverOldTopology(planTopoInfo, Collections.emptyList(), PlanScope.getDefaultInstance());
    }

    /**
     * Test that {@link TopologyPipeline#run(Object)} executes the VolumesDaysUnAttachedCalcStage before the
     * SettingsResolutionStage so that the user created policies on dynamic groups of Volumes using the Days Unattached
     * filter get properly applied to the member Volumes.
     *
     * @throws Pipeline.PipelineException if {@link Pipeline#run(Object)} encounters an error.
     * @throws InterruptedException if test is interrupted.
     */
    @Test
    @FeatureFlagTest(testCombos = {"USE_EXTENDABLE_PIPELINE_INPUT"})
    public void testPipelineRunExecutesVolumesDaysUnAttachedCalcStageBeforeSettingsResolutionStage()
        throws Pipeline.PipelineException, InterruptedException {
        // given
        final LivePipelineFactory lpf = livePipelineFactory(matrixInterface, licenseCheckClient);
        final TopologyPipeline<PipelineInput, TopologyBroadcastInfo> pipeline = lpf
            .liveTopology(liveTopoInfo, Collections.emptyList(), stitchingJournalFactory);
        // when
        pipeline.run(pipelineInput);
        // then
        final InOrder inOrder = Mockito.inOrder(historyVolumesListener, entitySettingsResolver);
        inOrder.verify(historyVolumesListener).getVolIdToLastAttachmentTime();
        inOrder.verify(entitySettingsResolver).resolveSettings(any(), any(), any(), any(), any(), any(), any());
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private LivePipelineFactory livePipelineFactory(@Nonnull final MatrixInterface matrixInterface,
                                                    @Nonnull final LicenseCheckClient licenseCheckClient) {
        return new LivePipelineFactory(
            topoBroadcastManager,
            policyManager,
            stitchingManager,
            mock(DiscoveredTemplateDeploymentProfileNotifier.class),
            mock(DiscoveredGroupUploader.class),
            mock(DiscoveredWorkflowUploader.class),
            mock(DiscoveredCloudCostUploader.class),
            mock(BilledCloudCostUploader.class),
            mock(AliasedOidsUploader.class),
            mock(DiscoveredPlanDestinationUploader.class),
            entitySettingsResolver,
            mock(EntitySettingsApplicator.class),
            environmentTypeInjector,
            mock(SearchResolver.class),
            groupServiceBlockingStub,
            reservationManager,
            mock(DiscoveredSettingPolicyScanner.class),
            mock(EntityValidator.class),
            mock(SupplyChainValidator.class),
            mock(DiscoveredClusterConstraintCache.class),
            mock(ApplicationCommodityKeyChanger.class),
            mock(ControllableManager.class),
            mock(HistoricalEditor.class),
            matrixInterface,
            mock(CachedTopology.class),
            probeActionCapabilitiesApplicatorEditor,
            mock(HistoryAggregator.class),
            licenseCheckClient,
            mock(ConsistentScalingConfig.class),
            mock(ActionConstraintsUploader.class),
            mock(ActionMergeSpecsUploader.class),
            mock(RequestAndLimitCommodityThresholdsInjector.class),
            ephemeralEntityEditor,
            reservationServiceStub,
            mock(GroupResolverSearchFilterResolver.class),
            mock(GroupScopeResolver.class),
            mock(EntityCustomTagsMerger.class),
            stalenessInformationProvider,
            10,
            historyVolumesListener,
            appSvcHistoryListener);
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
            mock(PostScopingTopologyEditor.class),
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
            mock(CloudMigrationPlanHelper.class),
            mock(ControllableManager.class),
            mock(ActionMergeSpecsUploader.class));

        return ppf;
    }
}
