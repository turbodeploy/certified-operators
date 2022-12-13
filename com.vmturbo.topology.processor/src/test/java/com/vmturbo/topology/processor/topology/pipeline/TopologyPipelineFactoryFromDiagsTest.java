package com.vmturbo.topology.processor.topology.pipeline;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOREST.Topology;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.pipeline.Pipeline;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig;
import com.vmturbo.stitching.poststitching.DiskCapacityCalculator;
import com.vmturbo.stitching.poststitching.SetAutoSetCommodityCapacityPostStitchingOperation.MaxCapacityCache;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.actions.ActionConstraintsUploader;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsUploader;
import com.vmturbo.topology.processor.api.TestApiServerConfig;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingConfig;
import com.vmturbo.topology.processor.controllable.ControllableManager;
import com.vmturbo.topology.processor.cost.AliasedOidsUploader;
import com.vmturbo.topology.processor.cost.BilledCloudCostUploader;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.cost.PriceTableUploader;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsHandler;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsHandlerTest;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
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
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.StaleOidManagerImpl;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.listeners.HistoryVolumesListener;
import com.vmturbo.topology.processor.listeners.TpAppSvcHistoryListener;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.planexport.DiscoveredPlanDestinationUploader;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.reservation.ReservationManager;
import com.vmturbo.topology.processor.rpc.TargetHealthRetriever;
import com.vmturbo.topology.processor.rpc.TargetHealthRetrieverTest;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidator;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.KvTargetDao;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetSpecAttributeExtractor;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.topology.ApplicationCommodityKeyChanger;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector;
import com.vmturbo.topology.processor.topology.EphemeralEntityEditor;
import com.vmturbo.topology.processor.topology.HistoricalEditor;
import com.vmturbo.topology.processor.topology.HistoryAggregator;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor;
import com.vmturbo.topology.processor.topology.RequestAndLimitCommodityThresholdsInjector;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;
import com.vmturbo.topology.processor.topology.TopologyRpcServiceTest;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

import common.HealthCheck.HealthState;

/**
 * <pre>
 * Tests that a live pipeline can be created and all stages are executed correctly.
 *
 * This test creates a live pipeline for a mock Topology after restoring a set of diags generated on
 * a test OVA with a test DC target.
 *
 * The diags are loaded into a <i>TopologyProcessorDiagnosticsHandler</i> instance, which will populate
 * all the needed objects to properly mock a discovery. The diags loaded by default are stored under
 * <i>src/test/resources/diags/compressed/diags_for_pipeline.zip</i>. Custom diags can be loaded using
 * the mvn arg <b>-DdiagsInputPath="custom/path.zip"</b>.
 *
 * The pipeline is built and run using the feature flag USE_EXTENDABLE_PIPELINE_INPUT value
 * <b>disabled</b> as per default.
 * Another instance of the pipeline is built and run using the feature flag USE_EXTENDABLE_PIPELINE_INPUT
 * value <b>enabled</b>.
 * The two pipelines summary output are dumped in a temp file and compared: if the files are different,
 * the test will fail.
 *
 * The pipeline summary output can be compared with a benchmark to validate the summary information,
 * setting the arg <i>-DdiagsBenchmarkCompare="true"</i>: in this case the pipeline output is compared
 * with a benchmark file stored under<i>src/test/resources/pipelineFromDiagsOut.txt</i>: if the files
 * are different, the test will fail.
 *
 * When the pipeline summary output changes permanently (for example when a new stage is added in the
 * pipeline, or when mocks in this test are replaced with default instances) the content of the benchmark
 * file can be updated: starting the test with mvn arg <b>-DdiagsBenchmarkSave="true"</b>.
 *
 * More info at https://vmturbo.atlassian.net/wiki/spaces/XD/pages/3540975638.
 * </pre>
 */
public class TopologyPipelineFactoryFromDiagsTest {

    private String diagsPath = "diags" + "/compressed/diags_for_pipeline.zip";
    private final String benchmarkPath = "pipelineFromDiagsOut.txt";
    private boolean generateNewBenchmark = false;
    private boolean compareWithBenchmark = false;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * JUnit rule to create a temp folder which last for the duration of the test.
     */
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * <pre>
     * Setup of the test properties input from input parameters.
     * The input maven properties to be checked are:
     *   - pipeline.diagsInput.path (-DdiagsInputPath)
     *   - pipeline.diagsBenchmark.save (-DdiagsBenchmarkSave)
     *   - pipeline.diagsBenchmark.compare (-DdiagsBenchmarkCompare)
     * </pre>
     */
    @Before
    public void setup() {
        // Parse input parameters and set properties according
        setProperties();
    }

    /**
     * Main test case. Steps executed:
     * - instantiate and run a legacy LivePipelineTest using EntityStore as input
     * - instantiate and run a refactored LivePipelineTest using StitchingContext as input
     * - compare the pipeline output from the above
     *
     * @throws Pipeline.PipelineException To satisfy compiler
     * @throws InterruptedException To satisfy compiler
     */
    @Test
    public void testPipeline() throws Pipeline.PipelineException, InterruptedException {
        logger.info("Testing pipeline with EntityStore as input");
        LivePipelineTest livePipelineTestLegacy = new LivePipelineTest();
        livePipelineTestLegacy.setup(true, false);
        final String livePipelineTestLegacyOutput = livePipelineTestLegacy.testPipelineFromDiags();

        logger.info("Testing pipeline with StitchingContext as input");
        LivePipelineTest livePipelineTestRefactored = new LivePipelineTest();
        livePipelineTestRefactored.setup(true, true);
        final String livePipelineTestRefactoredOutput = livePipelineTestRefactored.testPipelineFromDiags();

        logger.info("Comparing pipeline outputs");
        boolean sameOutput = compareFiles(livePipelineTestLegacyOutput,
                livePipelineTestRefactoredOutput);
        assertTrue("The output of the two pipeline tests does not match! ", sameOutput);
    }

    private boolean compareFiles(String filePath1, String filePath2) {
        boolean fileIdentical;
        try {
            Path path1 = Paths.get(filePath1);
            Path path2 = Paths.get(filePath2);
            InputStream inputStreamDump1 = Files.newInputStream(path1);
            InputStream inputStreamDump2 = Files.newInputStream(path2);
            fileIdentical = IOUtils.contentEquals(inputStreamDump1, inputStreamDump2);

            if (!fileIdentical) {
                logger.info("Files not matching.");
                // Compare byte-by-byte
                try (BufferedReader content1 = Files.newBufferedReader(path1);
                     BufferedReader content2 = Files.newBufferedReader(path2)) {

                    String line1;
                    String line2;
                    while ((line1 = content1.readLine()) != null) {
                        line2 = content2.readLine();
                        if (!line1.contentEquals(line2)) {
                            logger.info("Line\n" + line1 + "\nnot matching line\n" + line2);
                            return false;
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fileIdentical;
    }

    private void setProperties() {
        String diagsInputPath = System.getProperty("pipeline.diagsInput.path");
        if (diagsInputPath != null && !diagsInputPath.trim().equals("")) {
            diagsPath = diagsInputPath;
        }

        String diagsBenchmarkSave = System.getProperty("pipeline.diagsBenchmark.save");
        if (diagsBenchmarkSave != null && diagsBenchmarkSave.toLowerCase().equals("true")) {
            generateNewBenchmark = true;
        }

        String diagsBenchmarkCompare = System.getProperty("pipeline.diagsBenchmark.compare");
        if (diagsBenchmarkSave != null && diagsBenchmarkCompare.toLowerCase().equals("true")) {
            compareWithBenchmark = true;
        }
        logger.info("    pipeline.diagsInput.path        (-DdiagsInputPath)       : " + diagsPath);
        logger.info("    pipeline.diagsBenchmark.save    (-DdiagsBenchmarkSave)   : " + generateNewBenchmark);
        logger.info("    pipeline.diagsBenchmark.compare (-DdiagsBenchmarkCompare): " + compareWithBenchmark);
        boolean diagsFileExists = Files.exists(Paths.get(diagsPath)) || ResourcePath.getTestResource(getClass(), diagsPath).toFile().exists();
        assertTrue("Property pipeline.diagsInput.path not valid! ", diagsFileExists);
    }

    /**
     * Test class to instantiate and run a live pipeline.
     */
    private class LivePipelineTest {
        private final MatrixInterface matrixInterface = TheMatrix.instance();
        private final StitchingJournalFactory stitchingJournalFactory = StitchingJournalFactory.emptyStitchingJournalFactory();
        private final MapKeyValueStore keyValueStore = new MapKeyValueStore();
        private final FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();
        private final TestApiServerConfig testApiServerCfg = new TestApiServerConfig();
        private LicenseCheckClient licenseCheckClient;
        private ReservationServiceStub reservationServiceStub;
        private GroupServiceBlockingStub groupServiceBlockingStub;
        private StitchingManager stitchingManager;
        private EntityStore entityStore;
        private TargetStore targetStore;
        private ProbeStore probeStore;
        private LivePipelineFactory lpf;
        private StalenessInformationProvider stalenessInformationProvider;
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
        private TargetHealthRetriever targetHealthRetriever;
        private IdentityProvider identityProvider;
        private TargetDao targetDao;
        private TestIdentityStore targetIdentityStore;
        private TopologyProcessorDiagnosticsHandler handler;
        private GrpcTestServer grpcTestServerReservation;
        private GrpcTestServer grpcTestServerGroup;
        private final PersistentIdentityStore targetPersistentIdentityStore = mock(
                PersistentIdentityStore.class);
        private final Clock mockClock = Mockito.mock(Clock.class);
        private final BinaryDiscoveryDumper binaryDiscoveryDumper = mock(BinaryDiscoveryDumper.class);
        private final Scheduler scheduler = mock(Scheduler.class);
        private final DiscoveredGroupUploader groupUploader = mock(DiscoveredGroupUploader.class);
        private final DiscoveredCloudCostUploader discoveredCloudCostUploader = mock(
                DiscoveredCloudCostUploader.class);
        private final PriceTableUploader priceTableUploader = mock(PriceTableUploader.class);
        private final TopologyPipelineExecutorService pipelineExecutorService = mock(
                TopologyPipelineExecutorService.class);
        private final TargetStatusTracker targetStatusTracker = mock(TargetStatusTracker.class);
        private final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader =
                mock(DiscoveredTemplateDeploymentProfileUploader.class);

        /**
         * <pre>
         * Setup of the dependencies needed to restore the diags and run a live pipeline. This will:
         *   - setup grpc test servers (used for reservations and groups)
         *   - create default instances of non-mocked objects (such IdentityProvider, Target/Probe/Entity stores...)
         *   - create default instances used in stitching (mocking pre- and post-stitching operations)
         *   - mock all the remaining needed dependencies
         *   - restore the diags from a local exported file and populate the created objects
         * </pre>
         */
        @Before
        public void setup(boolean delayedDataHandling, boolean useExtendablePipelineInput) {
            // Setting feature flags
            setFeatureFlags(delayedDataHandling, useExtendablePipelineInput);
            // Create instances for the following dependency objects
            //
            // grpc test servers (for reservation and group)
            defaultGrpcTestServerReservation();
            defaultReservationServiceStub();
            defaultGrpcTestServerGroup();
            defaultGroupServiceBlockingStub();
            //
            defaultStalenessInformationProvider();
            defaultIdentityProvider();
            defaultProbeStore();
            defaultTargetStore();
            defaultEntityStore();
            // stitching context and manager
            defaultStitchingContext();
            defaultStitchingManager();

            // Mock instances for following dependency objects
            mockEnvironmentInjector();
            mockPolicyManager();
            mockHistoryVolumesListener();
            mockTpAppSvcDaysEmptyListener();
            mockEntitySettingsResolver();
            defaultBroadcastManager();
            mockReservationManager();
            mockEphemeralEntityEditor();
            mockProbeActionCapabilitiesApplicatorEditor();
            mockLicenseCheckClient();
            defaultTargetHealthRetriever();
            // Create the instance of the handler which will then be used to load diags
            defaultTopologyProcessorDiagnosticsHandler();

            // Load local static diags
            // this will populate all the objects created so far and will allow a pipeline execution
            restoreDiags();
        }

        private void defaultGrpcTestServerGroup() {
            grpcTestServerGroup = GrpcTestServer.newServer(new GroupServiceImplBase() {
                @Override
                public void createGroup(CreateGroupRequest request, StreamObserver<CreateGroupResponse> responseObserver) {
                    super.createGroup(request, responseObserver);
                }
            });
            try {
                grpcTestServerGroup.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void defaultGrpcTestServerReservation() {
            grpcTestServerReservation = testApiServerCfg.grpcTestServer();
        }

        private void defaultGroupServiceBlockingStub() {
            groupServiceBlockingStub = GroupServiceGrpc.newBlockingStub(grpcTestServerGroup.getChannel());
        }

        private void defaultReservationServiceStub() {
            reservationServiceStub = ReservationServiceGrpc.newStub(grpcTestServerReservation.getChannel());
        }

        private void defaultStalenessInformationProvider() {
            TargetHealth defaultTargetHealth = TargetHealth.newBuilder().setHealthState(HealthState.NORMAL).build();
            stalenessInformationProvider = targetOid -> defaultTargetHealth;
        }

        private void defaultIdentityProvider() {
            try {
                identityProvider = new IdentityProviderImpl(keyValueStore, mock(ProbeInfoCompatibilityChecker.class), 0L,
                        mock(IdentityDatabaseStore.class), 10, 0, mock(StaleOidManagerImpl.class),
                        false);
                identityProvider.getStore().initialize();
                identityProvider.waitForInitializedStore();
            } catch (InitializationException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void defaultProbeStore() {
            probeStore = new TestProbeStore(identityProvider);
        }

        private void defaultTargetStore() {
            targetDao = new KvTargetDao(keyValueStore, probeStore, mockClock);
            targetIdentityStore = new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore));
            targetStore = new CachingTargetStore(targetDao, probeStore, targetIdentityStore,
                    mockClock, binaryDiscoveryDumper);
        }

        private void defaultStitchingContext() {
            stitchingContext = entityStore.constructStitchingContext(stalenessInformationProvider);
        }

        private void defaultStitchingManager() {
            StitchingOperationStore stitchOperationStore = new StitchingOperationStore(new StitchingOperationLibrary(true), true);
            CpuCapacityStore cpuCapacityStore = cpuModel -> Optional.empty();
            StatsHistoryServiceBlockingStub statsServiceClient = StatsHistoryServiceGrpc.newBlockingStub(grpcTestServerReservation.getChannel());

            PreStitchingOperationLibrary preStitchingOpLib = new PreStitchingOperationLibrary();
            PostStitchingOperationLibrary postStitchingOpLib = new PostStitchingOperationLibrary(new CommodityPostStitchingOperationConfig(statsServiceClient, 30, 0),
                    //meaningless values
                    Mockito.mock(DiskCapacityCalculator.class), cpuCapacityStore, mockClock, 0, mock(MaxCapacityCache.class));

            stitchingManager = new StitchingManager(stitchOperationStore, preStitchingOpLib,
                    postStitchingOpLib, probeStore, targetStore, cpuCapacityStore);
        }

        private void defaultEntityStore() {
            entityStore = new EntityStore(targetStore, identityProvider, 0.3F, true,
                    Collections.singletonList(Mockito.mock(TopologyProcessorNotificationSender.class)),
                    mockClock, Collections.emptySet(), true);
        }

        private void defaultTopologyProcessorDiagnosticsHandler() {
            Map<String, BinaryDiagsRestorable> statefulEditors = Collections.emptyMap();
            handler = TopologyProcessorDiagnosticsHandlerTest.createInstance(targetStore,
                    targetPersistentIdentityStore, scheduler, entityStore, probeStore, groupUploader,
                    templateDeploymentProfileUploader, identityProvider, discoveredCloudCostUploader,
                    priceTableUploader, pipelineExecutorService, statefulEditors, binaryDiscoveryDumper,
                    targetStatusTracker, stalenessInformationProvider, mock(StaleOidManagerImpl.class),
                    targetHealthRetriever);
        }

        private void defaultTargetHealthRetriever() {
            SettingServiceBlockingStub settingServiceClient = SettingServiceGrpc.newBlockingStub(
                    grpcTestServerReservation.getChannel());
            IOperationManager operationManager = mock(IOperationManager.class);
            targetHealthRetriever = TargetHealthRetrieverTest.createInstance(operationManager,
                    targetStatusTracker, targetStore, probeStore, mockClock, settingServiceClient);
            this.entityStore.setTargetHealthRetriever(targetHealthRetriever);
        }

        private void defaultBroadcastManager() {
            List<Topology> responseList = new ArrayList<>();
            LinkedBlockingQueue<Status> statusQueue = new LinkedBlockingQueue<>(1);
            StreamObserver<com.vmturbo.common.protobuf.topology.TopologyDTO.Topology>
                    responseObserver = new StreamObserver<com.vmturbo.common.protobuf.topology.TopologyDTO.Topology>() {
                @Override
                public void onNext(com.vmturbo.common.protobuf.topology.TopologyDTO.Topology value) {
                    responseList.add(com.vmturbo.common.protobuf.topology.TopologyDTOREST.Topology.fromProto(
                            value));
                }

                @Override
                public void onError(Throwable t) {
                    statusQueue.offer(Status.fromThrowable(t));
                }

                @Override
                public void onCompleted() {
                    statusQueue.offer(Status.OK);
                }
            };
            topoBroadcastManager = TopologyRpcServiceTest.createInstance(responseObserver);
        }

        private void mockTpAppSvcDaysEmptyListener() {
            appSvcHistoryListener = mock(TpAppSvcHistoryListener.class);
            when(appSvcHistoryListener.getDaysEmptyInfosByAppSvc()).thenReturn(new HashMap<>());
        }

        private void mockEnvironmentInjector() {
            environmentTypeInjector = mock(EnvironmentTypeInjector.class);
            when(environmentTypeInjector.injectEnvironmentType(any())).thenReturn(new EnvironmentTypeInjector.InjectionSummary(0, 0, new HashMap<>()));
        }

        private void mockPolicyManager() {
            policyManager = mock(PolicyManager.class);
            when(policyManager.applyPolicies(any(), any(), any(), any(), any())).thenReturn(new PolicyApplicator.Results());
        }

        private void mockHistoryVolumesListener() {
            historyVolumesListener = mock(HistoryVolumesListener.class);
            when(historyVolumesListener.getVolIdToLastAttachmentTime()).thenReturn(new HashMap<>());
        }

        private void mockEntitySettingsResolver() {
            TopologyGraph<TopologyEntity> topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(new HashMap<>());
            entitySettingsResolver = mock(EntitySettingsResolver.class);
            when(entitySettingsResolver.resolveSettings(any(), any(), any(), any(), any(), any(),
                    any())).thenReturn(new GraphWithSettings(topologyGraph, new HashMap<>(), new HashMap<>()));
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
            when(probeActionCapabilitiesApplicatorEditor.applyPropertiesEdits(any())).thenReturn(new ProbeActionCapabilitiesApplicatorEditor.EditorSummary());
        }

        private void mockLicenseCheckClient() {
            licenseCheckClient = mock(LicenseCheckClient.class);
            when(licenseCheckClient.isDevFreemium()).thenReturn(false);
        }

        private void setFeatureFlags(boolean delayedDataHandling, boolean useExtendablePipelineInput) {
            // used by setControllableFlagForStaleEntities in StitchingManager
            if (delayedDataHandling) {
                featureFlagTestRule.enable(FeatureFlags.DELAYED_DATA_HANDLING);
            } else {
                featureFlagTestRule.disable(FeatureFlags.DELAYED_DATA_HANDLING);
            }

            if (useExtendablePipelineInput) {
                featureFlagTestRule.enable(FeatureFlags.USE_EXTENDABLE_PIPELINE_INPUT);
            } else {
                featureFlagTestRule.disable(FeatureFlags.USE_EXTENDABLE_PIPELINE_INPUT);
            }
        }

        /**
         * This will populate the objects created to instantiate the handler with the content of the
         * input diags.
         */
        private void restoreDiags() {
            logger.info("Restoring diags from " + diagsPath);
            try {
                File diagsFile = null;
                if (Files.exists(Paths.get(diagsPath))) {
                    diagsFile = new File(Paths.get(diagsPath).toUri());
                } else if (Files.exists(ResourcePath.getTestResource(getClass(), diagsPath))) {
                    diagsFile = ResourcePath.getTestResource(getClass(), diagsPath).toFile();
                }
                handler.restore(new FileInputStream(diagsFile), null);
            } catch (FileNotFoundException | DiagnosticsException e) {
                logger.error("Error restoring diags from " + diagsPath + " : " + e);
                throw new RuntimeException(e);
            }
        }

        /**
         * <pre>
         * Main test case. Steps executed:
         *   - define the live topology info
         *   - create an instance of the live pipeline
         *   - run the pipeline passing the entityStore populated from diags in the {@link #setup() setup()} method
         *   - dump the pipeline summary info in a temp file
         *   - compare the summary info in the dump file with the benchmark
         * </pre>
         *
         * @return The path of the tempFile
         * @throws Pipeline.PipelineException To satisfy compiler
         * @throws InterruptedException To satisfy compiler
         */
        @Test
        public String testPipelineFromDiags() throws Pipeline.PipelineException, InterruptedException {
            // Define topology info
            TopologyInfo liveTopoInfo = TopologyInfo.newBuilder()
                    .setTopologyContextId(11)
                    .setTopologyId(12)
                    .setTopologyType(TopologyType.REALTIME)
                    .build();
            // create the pipeline
            lpf = livePipelineFactory();
            TopologyPipeline<PipelineInput, TopologyBroadcastInfo> pipeline = lpf.liveTopology(
                    liveTopoInfo, Collections.emptyList(), stitchingJournalFactory);

            PipelineInput pipelineInput = PipelineInput.builder()
                    .setEntityStore(entityStore)
                    .setStalenessProvider(stalenessInformationProvider)
                    .build();

            // run the pipeline passing the entityStore populated from diags - in the setup() method -
            pipeline.run(pipelineInput);
            // dump the pipeline summary info in a temp file
            String dumpPath = dumpSummary(pipeline.getSummary().toString());
            if (compareWithBenchmark) {
                logger.info("Comparing the pipeline output with the benchmark");
                // compare summary info in the dump file with the benchmark
                assertTrue("The pipeline output does not match the benchmark!", compareToBenchmark(dumpPath));
            }
            return dumpPath;
        }

        @Nonnull
        private LivePipelineFactory livePipelineFactory() {

            return new LivePipelineFactory(topoBroadcastManager, policyManager, stitchingManager,
                    mock(DiscoveredTemplateDeploymentProfileNotifier.class), mock(DiscoveredGroupUploader.class), mock(DiscoveredWorkflowUploader.class),
                    mock(DiscoveredCloudCostUploader.class), mock(BilledCloudCostUploader.class),
                    mock(AliasedOidsUploader.class), mock(DiscoveredPlanDestinationUploader.class),
                    entitySettingsResolver, mock(EntitySettingsApplicator.class),
                    environmentTypeInjector, mock(SearchResolver.class), groupServiceBlockingStub,
                    reservationManager, mock(DiscoveredSettingPolicyScanner.class), mock(EntityValidator.class),
                    mock(SupplyChainValidator.class), mock(DiscoveredClusterConstraintCache.class),
                    mock(ApplicationCommodityKeyChanger.class), mock(ControllableManager.class),
                    mock(HistoricalEditor.class), matrixInterface, mock(CachedTopology.class),
                    probeActionCapabilitiesApplicatorEditor, mock(HistoryAggregator.class),
                    licenseCheckClient, mock(ConsistentScalingConfig.class), mock(ActionConstraintsUploader.class), mock(ActionMergeSpecsUploader.class),
                    mock(RequestAndLimitCommodityThresholdsInjector.class), ephemeralEntityEditor,
                    reservationServiceStub, mock(GroupResolverSearchFilterResolver.class), mock(GroupScopeResolver.class), mock(EntityCustomTagsMerger.class),
                    stalenessInformationProvider, 10, historyVolumesListener, appSvcHistoryListener);
        }

        /**
         * <pre>
         * Replace any text array
         *   such <i>"DROPPED=(settingPolicyEditors, groupResolver, resolvedGroups)"</i>
         *      or <i>"REQUIRED=(groupResolver[cacheSize=0], settingPolicyEditors[size=0], resolvedGroups[size=0])"</i>
         * with their alphabetically sorted version
         *      such <i>"DROPPED=(groupResolver, resolvedGroups, settingPolicyEditors)"</i>
         *      or <i>"REQUIRED=(groupResolver[cacheSize=0], resolvedGroups[size=0], settingPolicyEditors[size=0])"</i>.
         * The default regex pattern in use will catch any text within parenthesis, but it could be passed with <b>p</b>
         * something more precise if needed,
         *      such <i>"DROPPED=\\(([ A-Za-z,]+)\\)");"</i>
         *      or  <i>"REQUIRED=\\(([A-Za-z,\\[\\]=0-9]+)\\)"</i>
         * </pre>
         *
         * @param p regex patters to use to find text arrays. If null will check any text
         *         array in parentheses using
         *         default regex <i>"\\(([^()]*)\\)"</i>
         * @param input input text to process
         * @return input text with text arrays sorted
         */
        private String sortTextArrays(@Nullable Pattern p, String input) {
            if (p == null) {
                p = Pattern.compile("\\(([^()]*)\\)");
            }
            Matcher m = p.matcher(input);
            String sortedInput = input;
            String tmpString;
            while (m.find()) {
                String matched = m.group(1);
                String[] c = matched.split(", ");
                if (c.length > 1) {
                    Arrays.sort(c);
                    String sortedString = String.join(", ", c);
                    tmpString = sortedInput.replace(matched, sortedString);
                    sortedInput = tmpString;
                }
            }
            return sortedInput;
        }

        /**
         * Create a temporary dump file with the cleaned output of the pipeline info to compare with
         * a benchmark.
         *
         * @param inputSummary content of the dump summary file
         * @return path of the temporary file
         */
        private String dumpSummary(String inputSummary) {
            String cleanedInput;

            // remove durations and timestamps
            cleanedInput = inputSummary.replaceAll("[0-9]+(.[0-9]+)*[a-z]+", "@IGNORE").replaceAll(
                    "[0-9T.,-:]+Z", "@IGNORE");
            cleanedInput = sortTextArrays(null, cleanedInput);
            File f1;
            String path;
            try {
                String fileName = benchmarkPath.split("/")[benchmarkPath.split("/").length - 1];
                fileName = fileName.split("\\.")[0] + "_" + System.currentTimeMillis() + "." + fileName.split("\\.")[1];
                f1 = tmpFolder.newFile(fileName);
                path = f1.getAbsolutePath();
                try (FileOutputStream fos = new FileOutputStream(path)) {
                    byte[] arr = cleanedInput.getBytes();
                    fos.write(arr);
                    logger.info("Temp dump file saved under " + path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Expecting this to be false
            if (generateNewBenchmark) {
                logger.info("Generating new benchmark file");
                saveNewBenchmark(cleanedInput);
            }

            return path;
        }

        /**
         * Compares the content of the file at <b>dumpFilePath</b> with a benchmark saved under
         * <b>benchmarkPath</b>.
         *
         * @param dumpFilePath path of the temp file with the content of the pipeline
         *         summary
         *         info to compare
         * @return <b>false</b> if the files have different contents
         */
        private boolean compareToBenchmark(String dumpFilePath) {
            boolean fileIdentical;
            String path1 = ResourcePath.getTestResource(getClass(), benchmarkPath).toString();
            String path2 = Paths.get(dumpFilePath).toString();
            fileIdentical = compareFiles(path1, path2);
            return fileIdentical;
        }

        /**
         * <pre>
         * Replace the content of the current benchmark file (under <b>benchmarkPath</b>) with the input
         * content.
         * At runtime the benchmark is under
         * <i>xl/com/vmturbo/topology/processor/target/test-classes/</i>.
         * However, the original destination path should be
         * <i>xl/com/vmturbo/topology/processor/src/test/resources/</i>,
         * as from here it will be generated a new <i>target/test-classes</i> content when building.
         * </pre>
         *
         * @param benchmarkContent content of the new benchmark (to replace the original
         *         one)
         */
        private void saveNewBenchmark(String benchmarkContent) {
            String targetPath = ResourcePath.getTestResource(getClass(), benchmarkPath)
                    .toAbsolutePath()
                    .toString();
            // replace in "target" compiled classes path: xl/com/vmturbo/topology/processor/target/test-classes/
            try (FileOutputStream fos = new FileOutputStream(targetPath)) {
                byte[] arr = benchmarkContent.getBytes();
                fos.write(arr);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String srcPath = targetPath.replace("target/test-classes", "src/test/resources");
            // replace in "test" src path: xl/com/vmturbo/topology/processor/src/test/resources/
            try (FileOutputStream fos = new FileOutputStream(srcPath)) {
                byte[] arr = benchmarkContent.getBytes();
                fos.write(arr);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            logger.info("New benchmark saved under " + targetPath + " and " + srcPath);
        }
    }
}
