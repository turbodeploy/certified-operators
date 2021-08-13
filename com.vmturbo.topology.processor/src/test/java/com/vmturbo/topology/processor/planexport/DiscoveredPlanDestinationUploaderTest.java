package com.vmturbo.topology.processor.planexport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.StoreDiscoveredPlanDestinationsResponse;
import com.vmturbo.common.protobuf.plan.PlanExportServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanExportServiceGrpc.PlanExportServiceStub;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.PlanDestinationData;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * Test the DiscoveredPlanDestinationUploader.
 */
public class DiscoveredPlanDestinationUploaderTest {
    private static final String LOCAL_NAME = "LocalName";
    private static final String PATH_PREFIX = "/path/to";
    private static final String OTHER_NAMESPACE = "other_namespace";

    private PlanExportServiceStub planExportServiceStub;

    /**
     * A rule to enable testing of PlanExportService RPC calls in-process.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    /**
     * A stream observer which recors all destinations uploaded for testing purposes.
     */
    private static class TestPlanDestinationsObserver implements
        StreamObserver<PlanDestination> {

        private final StreamObserver<StoreDiscoveredPlanDestinationsResponse> responseObserver;
        private final List<PlanDestination> destinations = new ArrayList<>();
        private Throwable error = null;

        /**
         * Constructor for the upload observer.
         *
         * @param responseObserver the observer which notifies client of any result
         */
        TestPlanDestinationsObserver(
            StreamObserver<StoreDiscoveredPlanDestinationsResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final PlanDestination record) {
            destinations.add(record);
        }

        @Override
        public void onError(final Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
            responseObserver.onNext(StoreDiscoveredPlanDestinationsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        /**
         * Get the destinations that were uploaded to this mock service.
         *
         * @return the destinations
         */
        @Nonnull
        List<PlanDestination> getUploadedDestinations() {
            return destinations;
        }

        /**
         * Get any error that was recorded.
         *
         * @return a throwable representing an error, if any, or null.
         */
        @Nullable
        Throwable getError() {
            return error;
        }
    }

    /**
     * A mock plan export service that records the exported destinations.
     */
    private static class TestingPlanExportService
        extends PlanExportServiceGrpc.PlanExportServiceImplBase {

        TestPlanDestinationsObserver lastObserver;

        @Override
        public StreamObserver<com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination> storeDiscoveredPlanDestinations(
            StreamObserver<StoreDiscoveredPlanDestinationsResponse> responseObserver) {
            lastObserver = new TestPlanDestinationsObserver(responseObserver);
            return lastObserver;
        }

        TestPlanDestinationsObserver getLastObserver() {
            return lastObserver;
        }
    }

    private final TestingPlanExportService testingService = new TestingPlanExportService();

    private final PlanExportServiceGrpc.PlanExportServiceImplBase serviceImpl =
        mock(PlanExportServiceGrpc.PlanExportServiceImplBase.class, delegatesTo(testingService));

    /**
     * Prepare to run tests.
     *
     * @throws Exception should not happen
     */
    @Before
    public void setUp() throws Exception {
        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
            .forName(serverName).directExecutor().addService(serviceImpl).build().start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcCleanup.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());

        planExportServiceStub = PlanExportServiceGrpc.newStub(channel);
    }

    /**
     * Test that recording of plan destinations from discovery correctly handles updates,
     * including updates where the target has no destinations.
     */
    @Test
    public void testRecordingDestinations() {
        final DiscoveredPlanDestinationUploader uploader =
            new DiscoveredPlanDestinationUploader(planExportServiceStub);

        // Record some destinations for target 1

        uploader.recordPlanDestinations(1L, Arrays.asList(
            makePlanDestinationNME("Dest1A", false),
            makeOtherNME("cloudService1", NonMarketEntityType.CLOUD_SERVICE),
            makePlanDestinationNME("Dest1B", true)));

        // Record some destinations for target 2
        uploader.recordPlanDestinations(2L, Arrays.asList(
            makeOtherNME("cloudService2", NonMarketEntityType.CLOUD_SERVICE),
            makePlanDestinationNME("Dest2A", false),
            makePlanDestinationNME("Dest2B", true),
            makeOtherNME("commitment2", NonMarketEntityType.CLOUD_COMMITMENT),
            makePlanDestinationNME("Dest2C", false)));

        // Record some destinations for target 3
        uploader.recordPlanDestinations(3L, Arrays.asList(
            makePlanDestinationNME("Dest3A", false),
            makeOtherNME("cloudService3", NonMarketEntityType.CLOUD_SERVICE)));

        // Record new destinations for target 1
        uploader.recordPlanDestinations(1L, Arrays.asList(
            makePlanDestinationNME("Dest1C", false),
            makeOtherNME("commitment1", NonMarketEntityType.CLOUD_COMMITMENT),
            makePlanDestinationNME("Dest1D", true)));

        // Record new (empty) destinations for target 3
        uploader.recordPlanDestinations(3L, Collections.emptyList());

        // Record no destinations for target 4
        uploader.recordPlanDestinations(4L, Collections.emptyList());

        // Get the currently recorded data
        Map<Long, List<NonMarketEntityDTO>> snapshot =
            uploader.getPlanDestinationDataByTargetIdSnapshot();

        // Target 1 should have data
        List<NonMarketEntityDTO> targetData1 = snapshot.get(1L);
        assertNotNull(targetData1);

        // It should have 2 destinations. Other kinds of NMEs are not recorded.
        assertEquals(2, targetData1.size());
        assertTrue(containsDestination("Dest1C", targetData1));
        assertTrue(containsDestination("Dest1D", targetData1));

        // Target 2 should have data
        List<NonMarketEntityDTO> targetData2 = snapshot.get(2L);
        assertNotNull(targetData2);

        // It should have 3 destinations. Other kinds of NMEs are not recorded.
        assertEquals(3, targetData2.size());
        assertTrue(containsDestination("Dest2A", targetData2));
        assertTrue(containsDestination("Dest2B", targetData2));
        assertTrue(containsDestination("Dest2C", targetData2));

        // Targets 3 and 4 should have no data
        assertFalse(snapshot.containsKey(3L));
        assertFalse(snapshot.containsKey(4L));
    }

    /**
     * Test that when a target is removed, that the corresponding data for the target
     * is removed.
     */
    @Test
    public void testRemovingTarget() {
        DiscoveredPlanDestinationUploader uploader =
            new DiscoveredPlanDestinationUploader(planExportServiceStub);

        // Record some destinations for target 1
        uploader.recordPlanDestinations(1L, Arrays.asList(
            makePlanDestinationNME("Dest1A", false),
            makeOtherNME("cloudService1", NonMarketEntityType.CLOUD_SERVICE),
            makePlanDestinationNME("Dest1B", true)));

        // Record some destinations for target 2
        uploader.recordPlanDestinations(2L, Arrays.asList(
            makeOtherNME("cloudService2", NonMarketEntityType.CLOUD_SERVICE),
            makePlanDestinationNME("Dest2A", false),
            makePlanDestinationNME("Dest2B", true),
            makeOtherNME("commitment2", NonMarketEntityType.CLOUD_COMMITMENT),
            makePlanDestinationNME("Dest2C", false)));

        // Record some destinations for target 3
        uploader.recordPlanDestinations(3L, Arrays.asList(
            makePlanDestinationNME("Dest3A", false),
            makeOtherNME("cloudService3", NonMarketEntityType.CLOUD_SERVICE)));

        // Remove target 2
        uploader.targetRemoved(2L);

        Map<Long, List<NonMarketEntityDTO>> snapshot =
            uploader.getPlanDestinationDataByTargetIdSnapshot();

        // Target 1 should have data
        List<NonMarketEntityDTO> targetData1 = snapshot.get(1L);
        assertNotNull(targetData1);

        // It should have 2 destinations. Other kinds of NMEs are not recorded.
        assertEquals(2, targetData1.size());
        assertTrue(containsDestination("Dest1A", targetData1));
        assertTrue(containsDestination("Dest1B", targetData1));

        // Target 2 should have no data since it was removed
        assertFalse(snapshot.containsKey(2L));

        // Target 3 should have data
        List<NonMarketEntityDTO> targetData3 = snapshot.get(3L);
        assertNotNull(targetData3);

        // It should have 3 destinations. Other kinds of NMEs are not recorded.
        assertEquals(1, targetData3.size());
        assertTrue(containsDestination("Dest3A", targetData3));
    }

    /**
     * Test that destinations are uploaded to the Plan Export service correctly.
     *
     * @throws Throwable should not happen, indicates a test failure
     */
    @Test
    public void testUploadDestinations() throws Throwable {
        final DiscoveredPlanDestinationUploader uploader =
            new DiscoveredPlanDestinationUploader(planExportServiceStub);

        // Record some destinations for target 1

        uploader.recordPlanDestinations(1L, Arrays.asList(
            makePlanDestinationNME("Dest1A", false),
            makeOtherNME("cloudService1", NonMarketEntityType.CLOUD_SERVICE),
            makePlanDestinationNME("Dest1B", true)));

        // Record some destinations for target 2
        uploader.recordPlanDestinations(2L, Arrays.asList(
            makeOtherNME("cloudService2", NonMarketEntityType.CLOUD_SERVICE),
            makePlanDestinationNME("Dest2A", false),
            makePlanDestinationNME("Dest2B", true),
            makeOtherNME("commitment2", NonMarketEntityType.CLOUD_COMMITMENT),
            makePlanDestinationNME("Dest2C", false)));

        // Mocked business accounts for the targets
        TopologyStitchingEntity account1 = makeAccountTSE(1L, 1001L);
        TopologyStitchingEntity account2 = makeAccountTSE(2L, 2002L);

        Map<Long, List<TopologyStitchingEntity>> accountsByTarget = new HashMap<>();
        accountsByTarget.put(account1.getTargetId(), Collections.singletonList(account1));
        accountsByTarget.put(account2.getTargetId(), Collections.singletonList(account2));

        StitchingContext stitchingContext = mock(StitchingContext.class);
        when(stitchingContext.getEntitiesByEntityTypeAndTarget())
            .thenReturn(Collections.singletonMap(EntityType.BUSINESS_ACCOUNT, accountsByTarget));

        uploader.uploadPlanDestinations(stitchingContext);

        TestPlanDestinationsObserver observer = testingService.getLastObserver();

        // If any error was raised, fail the test
        if (observer.getError() != null) {
            throw observer.getError();
        }

        // Get the actual PlanDestination DTOs that were uploaded to the mock plan export service
        List<PlanDestination> uploaded = observer.getUploadedDestinations();
        assertEquals(5, uploaded.size());

        // Break them out by ID for ease of testing, and test data common to all destinations
        Map<String, PlanDestination> destinationsById = new HashMap<>();
        for (PlanDestination destination : uploaded) {
            // Verify that DEFAULT namespace properties were passed through
            assertEquals(1, destination.getPropertyMapCount());
            assertEquals(PATH_PREFIX + destination.getExternalId(),
                destination.getPropertyMapMap().get(LOCAL_NAME));

            destinationsById.put(destination.getExternalId(), destination);
        }

        // Verify the data that varies between destinations
        checkDestination(destinationsById, "Dest1A", false, account1);
        checkDestination(destinationsById, "Dest1B", true, account1);
        checkDestination(destinationsById, "Dest2A", false, account2);
        checkDestination(destinationsById, "Dest2B", true, account2);
        checkDestination(destinationsById, "Dest2C", false, account2);
    }

    private boolean containsDestination(@Nonnull String id, List<NonMarketEntityDTO> entities) {
        return entities.stream()
            .filter(e -> e.getEntityType() == NonMarketEntityType.PLAN_DESTINATION)
            .filter(NonMarketEntityDTO::hasPlanDestinationData)
            .anyMatch(e -> e.getId().equals(id));
    }

    private NonMarketEntityDTO makePlanDestinationNME(@Nonnull String id,
                                                      boolean hasData) {
        return NonMarketEntityDTO.newBuilder()
            .setEntityType(NonMarketEntityType.PLAN_DESTINATION)
            .setId(id)
            .setDisplayName("Destination " + id)

            // Simulate some entity properties
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                .setName(LOCAL_NAME)
                .setValue(PATH_PREFIX + id))
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace(OTHER_NAMESPACE)
                .setName("foo")
                .setValue("bar"))

            .setPlanDestinationData(PlanDestinationData.newBuilder()
                .setHasExportedData(hasData))
            .build();
    }

    private NonMarketEntityDTO makeOtherNME(@Nonnull String id,
                                            @Nonnull NonMarketEntityType type) {
        return NonMarketEntityDTO.newBuilder()
            .setEntityType(type)
            .setId(id)
            .build();
    }

    private TopologyStitchingEntity makeAccountTSE(long targetId, long oid) {
        EntityDTO.Builder entityBuilder = EntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT);

        return new TopologyStitchingEntity(entityBuilder, oid, targetId, 0L);
    }

    private void checkDestination(@Nonnull Map<String, PlanDestination> destinations,
                                  @Nonnull String externalId,
                                  boolean hasData,
                                  @Nonnull TopologyStitchingEntity expectedAccount) {
        // Verify that the destination was uploaded and has the expected setting
        // for whether it has uploaded data or not
        PlanDestination destination = destinations.get(externalId);
        assertNotNull(destination);
        assertTrue(destination.hasHasExportedData());
        assertEquals(hasData, destination.getHasExportedData());

        // Verify that the destination is associated with the correct account
        assertTrue(destination.hasCriteria());
        assertEquals(expectedAccount.getOid(), destination.getCriteria().getAccountId());
    }
}
