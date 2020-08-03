package com.vmturbo.cloud.commitment.analysis.runtime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Optional;

import com.google.common.collect.Lists;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext.AnalysisContextFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext.DefaultAnalysisContextFactory;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher.CloudCommitmentSpecMatcherFactory;
import com.vmturbo.cloud.commitment.analysis.topology.BillingFamilyRetriever;
import com.vmturbo.cloud.commitment.analysis.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.commitment.analysis.topology.MinimalCloudTopology;
import com.vmturbo.cloud.commitment.analysis.topology.MinimalCloudTopology.MinimalCloudTopologyFactory;
import com.vmturbo.cloud.commitment.analysis.topology.MinimalEntityCloudTopology.DefaultMinimalEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.ClassifiedDemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.ReservedInstancePurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.TopologyReference;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Testing the CloudCommitmentAnalysisContext.
 */
public class CloudCommitmentAnalysisContextTest {

    private final RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    private RepositoryServiceBlockingStub repositoryService;

    private final BillingFamilyRetriever billingFamilyRetriever = mock(BillingFamilyRetriever.class);
    private final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory =
            mock(BillingFamilyRetrieverFactory.class);

    private final MinimalCloudTopologyFactory<MinimalEntity> minimalCloudTopologyFactory =
            new DefaultMinimalEntityCloudTopologyFactory(billingFamilyRetrieverFactory);

    private final TopologyEntityCloudTopologyFactory fullCloudTopologyFactory =
            mock(TopologyEntityCloudTopologyFactory.class);

    private final CloudCommitmentSpecMatcherFactory cloudCommitmentSpecMatcherFactory =
            mock(CloudCommitmentSpecMatcherFactory.class);

    private final long analysisSegmentInterval = 543;
    private final TemporalUnit analysisSegmentUnit = ChronoUnit.HOURS;
    private final StaticAnalysisConfig staticAnalysisConfig = ImmutableStaticAnalysisConfig.builder()
            .analysisSegmentInterval(analysisSegmentInterval)
            .analysisSegmentUnit(analysisSegmentUnit)
            .build();

    private AnalysisContextFactory analysisContextFactory;

    /**
     * Setting the grpc test server.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    /**
     * Setup the test.
     *
     * @throws Exception An exception.
     */
    @Before
    public void setup() throws Exception {
        when(billingFamilyRetrieverFactory.newInstance()).thenReturn(billingFamilyRetriever);

        // Generate a unique in-process server name.
        final String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(
                InProcessServerBuilder
                        .forName(serverName)
                        .directExecutor()
                        .addService(repositoryServiceMole)
                        .build()
                        .start());

        // Create a client channel and register for automatic graceful shutdown.
        final ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder
                        .forName(serverName)
                        .directExecutor()
                        .build());

        repositoryService = RepositoryServiceGrpc.newBlockingStub(channel);
        analysisContextFactory = new DefaultAnalysisContextFactory(
                repositoryService,
                minimalCloudTopologyFactory,
                fullCloudTopologyFactory,
                cloudCommitmentSpecMatcherFactory,
                staticAnalysisConfig);
    }

    /**
     * Testing the log marker for analysis.
     */
    @Test
    public void testLogMarker() {

        final String analysisTag = "analysisTagTest";
        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(123L)
                .setAnalysisTag(analysisTag)
                .setCreationTime(Instant.now().toEpochMilli())
                .build();

        final CloudCommitmentAnalysisContext analysisContext = analysisContextFactory.createContext(
                analysisInfo, TestUtils.createBaseConfig());

        assertThat(analysisContext.getLogMarker(), equalTo(String.format("[123|%s]", analysisTag)));
    }

    /**
     * Testing the source topology with a reference.
     */
    @Test
    public void createSourceTopologyWithReference() {

        // setup analysis info
        final long contextId = 456L;
        final long topologyId = 789L;
        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(123L)
                .setAnalysisTag("analysisTag")
                .setCreationTime(Instant.now().toEpochMilli())
                .setTopologyReference(TopologyReference.newBuilder()
                        .setTopologyContextId(contextId)
                        .setTopologyId(topologyId))
                .build();

        // setup repository response
        final MinimalEntity minimalEntityA = MinimalEntity.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final MinimalEntity minimalEntityB = MinimalEntity.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final PartialEntityBatch partialEntityBatch = PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setMinimal(minimalEntityA))
                .addEntities(PartialEntity.newBuilder().setMinimal(minimalEntityB))
                .build();

        when(repositoryServiceMole.retrieveTopologyEntities(any())).thenReturn(Lists.newArrayList(partialEntityBatch));

        // get the context.
        final CloudCommitmentAnalysisContext analysisContext = analysisContextFactory.createContext(
                analysisInfo, TestUtils.createBaseConfig());
        final MinimalCloudTopology<MinimalEntity> cloudTopology = analysisContext.getSourceCloudTopology();

        // capture the repository request
        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> repositoryRequestCaptor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repositoryServiceMole).retrieveTopologyEntities(repositoryRequestCaptor.capture());
        final RetrieveTopologyEntitiesRequest actualRepositoryRequest = repositoryRequestCaptor.getValue();

        // setup expected repository request
        final RetrieveTopologyEntitiesRequest expectedRepositoryRequest  = RetrieveTopologyEntitiesRequest.newBuilder()
                .setReturnType(Type.MINIMAL)
                .setTopologyType(TopologyType.SOURCE)
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId)
                .build();

        // Check assertions
        assertThat(actualRepositoryRequest, equalTo(expectedRepositoryRequest));

        assertThat(cloudTopology.getEntities().values(), hasSize(2));
        assertThat(cloudTopology.getEntities().values(), containsInAnyOrder(minimalEntityA, minimalEntityB));
    }

    /**
     * Test the source topology without a reference.
     */
    @Test
    public void createSourceTopologyWithoutReference() {

        // setup analysis info
        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(123L)
                .setAnalysisTag("analysisTag")
                .setCreationTime(Instant.now().toEpochMilli())
                .build();

        // setup repository response
        final MinimalEntity minimalEntityA = MinimalEntity.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final MinimalEntity minimalEntityB = MinimalEntity.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final PartialEntityBatch partialEntityBatch = PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setMinimal(minimalEntityA))
                .addEntities(PartialEntity.newBuilder().setMinimal(minimalEntityB))
                .build();

        when(repositoryServiceMole.retrieveTopologyEntities(any())).thenReturn(Lists.newArrayList(partialEntityBatch));

        // get the context.
        final CloudCommitmentAnalysisContext analysisContext = analysisContextFactory.createContext(
                analysisInfo, TestUtils.createBaseConfig());
        final MinimalCloudTopology<MinimalEntity> cloudTopology = analysisContext.getSourceCloudTopology();

        // capture the repository request
        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> repositoryRequestCaptor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repositoryServiceMole).retrieveTopologyEntities(repositoryRequestCaptor.capture(), any());

        // setup expected repository request
        final RetrieveTopologyEntitiesRequest expectedRepositoryRequest  = RetrieveTopologyEntitiesRequest.newBuilder()
                .setReturnType(Type.MINIMAL)
                .setTopologyType(TopologyType.SOURCE)
                .build();

        // Check assertions
        final RetrieveTopologyEntitiesRequest actualRepositoryRequest = repositoryRequestCaptor.getValue();
        assertThat(actualRepositoryRequest, equalTo(expectedRepositoryRequest));

        assertThat(cloudTopology.getEntities().values(), hasSize(2));
        assertThat(cloudTopology.getEntities().values(), containsInAnyOrder(minimalEntityA, minimalEntityB));
    }

    /**
     * Test the analysis segment on creation of the context.
     */
    @Test
    public void testAnalysisSegment() {

        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(123L)
                .setAnalysisTag("analysisTag")
                .setCreationTime(Instant.now().toEpochMilli())
                .build();

        final CloudCommitmentAnalysisContext analysisContext = analysisContextFactory.createContext(
                analysisInfo, TestUtils.createBaseConfig());

        assertThat(analysisContext.getAnalysisSegmentInterval(), equalTo(analysisSegmentInterval));
        assertThat(analysisContext.getAnalysisSegmentUnit(), equalTo(analysisSegmentUnit));
    }

    /**
     * Testing the cloud topology associated with the context.
     */
    @Test
    public void testGetCloudTierTopology() {
        // setup analysis info
        final long contextId = 456L;
        final long topologyId = 789L;
        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(123L)
                .setAnalysisTag("analysisTag")
                .setCreationTime(Instant.now().toEpochMilli())
                .setTopologyReference(TopologyReference.newBuilder()
                        .setTopologyContextId(contextId)
                        .setTopologyId(topologyId))
                .build();

        // setup repository response
        final TopologyEntityDTO entityA = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .build();

        final TopologyEntityDTO entityB = TopologyEntityDTO.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .build();

        final PartialEntityBatch partialEntityBatch = PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(entityA))
                .addEntities(PartialEntity.newBuilder().setFullEntity(entityB))
                .build();

        when(repositoryServiceMole.retrieveTopologyEntities(any())).thenReturn(Lists.newArrayList(partialEntityBatch));

        // setup cloud topology mock
        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(fullCloudTopologyFactory.newCloudTopology(any())).thenReturn(cloudTopology);

        // create and invoke the analysis context
        final CloudCommitmentAnalysisContext analysisContext = analysisContextFactory.createContext(
                analysisInfo, TestUtils.createBaseConfig());
        final CloudTopology<TopologyEntityDTO> actualCloudTopology = analysisContext.getCloudTierTopology();

        // capture the repository request
        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> repositoryRequestCaptor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);

        verify(repositoryServiceMole).retrieveTopologyEntities(repositoryRequestCaptor.capture());
        final RetrieveTopologyEntitiesRequest actualRepositoryRequest = repositoryRequestCaptor.getValue();

        // setup expected repository request
        final RetrieveTopologyEntitiesRequest expectedRepositoryRequest  = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId)
                .setReturnType(Type.FULL)
                .setTopologyType(TopologyType.SOURCE)
                .addEntityType(EntityType.COMPUTE_TIER_VALUE)
                .build();

        // Check assertions
        assertThat(actualRepositoryRequest, equalTo(expectedRepositoryRequest));

        assertThat(actualCloudTopology, equalTo(cloudTopology));
    }

    /**
     * Testing the analysis start time.
     */
    @Test
    public void testAnalysisStartTime() {

        final Instant firstStartTime = Instant.now().minus(10, ChronoUnit.DAYS);
        final Instant secondStartTime = Instant.now();

        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(123L)
                .setAnalysisTag("analysisTag")
                .setCreationTime(Instant.now().toEpochMilli())
                .build();

        final CloudCommitmentAnalysisContext analysisContext = analysisContextFactory.createContext(
                analysisInfo, TestUtils.createBaseConfig());

        assertTrue(analysisContext.setAnalysisStartTime(firstStartTime));
        assertFalse(analysisContext.setAnalysisStartTime(secondStartTime));
        assertThat(analysisContext.getAnalysisStartTime(), equalTo(Optional.of(firstStartTime)));
    }

    @Test
    public void testGetCloudCommitmentSpecMatcher() {

        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(123L)
                .setAnalysisTag("analysisTag")
                .setCreationTime(Instant.now().toEpochMilli())
                .build();

        // Setup received topology from repository
        final PartialEntityBatch partialEntityBatch = PartialEntityBatch.newBuilder()
                .build();
        when(repositoryServiceMole.retrieveTopologyEntities(any())).thenReturn(Lists.newArrayList(partialEntityBatch));

        // setup cloud topology mock
        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(fullCloudTopologyFactory.newCloudTopology(any())).thenReturn(cloudTopology);


        final CloudCommitmentSpecMatcher cloudCommitmentSpecMatcher = mock(CloudCommitmentSpecMatcher.class);
        when(cloudCommitmentSpecMatcherFactory.createSpecMatcher(any(), any()))
                .thenReturn(cloudCommitmentSpecMatcher);

        // create and invoke the analysis context
        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig()
                .toBuilder()
                .setPurchaseProfile(CommitmentPurchaseProfile.newBuilder()
                        .addScope(ClassifiedDemandScope.newBuilder()
                                .setScope(DemandScope.newBuilder())
                                .addAllocatedDemandClassification(AllocatedDemandClassification.ALLOCATED))
                        .setRecommendationSettings(RecommendationSettings.newBuilder()
                                .setIncludeTerminatedEntities(false)
                                .setIncludeSuspendedEntities(false))
                        .setRiPurchaseProfile(ReservedInstancePurchaseProfile.newBuilder()))
                .build();

        final CloudCommitmentAnalysisContext analysisContext = analysisContextFactory.createContext(
                analysisInfo, analysisConfig);
        final CloudCommitmentSpecMatcher actualSpecMatcher = analysisContext.getCloudCommitmentSpecMatcher();

        // check the args passed to the spec matcher factor
        final ArgumentCaptor<CloudTopology> cloudTopologyCaptor = ArgumentCaptor.forClass(CloudTopology.class);
        final ArgumentCaptor<CommitmentPurchaseProfile> purchaseProfileCaptor =
                ArgumentCaptor.forClass(CommitmentPurchaseProfile.class);
        verify(cloudCommitmentSpecMatcherFactory).createSpecMatcher(
                cloudTopologyCaptor.capture(),
                purchaseProfileCaptor.capture());

        assertThat(cloudTopologyCaptor.getValue(), equalTo(cloudTopology));
        assertThat(purchaseProfileCaptor.getValue(), equalTo(analysisConfig.getPurchaseProfile()));

        // check that the spec matcher is correct
        assertThat(actualSpecMatcher, equalTo(cloudCommitmentSpecMatcher));
    }

}
