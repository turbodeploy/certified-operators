package com.vmturbo.components.test.performance.group;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Iterators;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest.SettingsChunk;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.ServiceHealthCheck.BasicServiceHealthCheck;
import com.vmturbo.group.api.GroupClientConfig;

@Alert({"group_entity_setting_update_duration_seconds_sum/1min",
    "group_entity_setting_query_duration_seconds_sum/1min",
    "jvm_memory_bytes_used_max"})
public class GroupPerformanceTest {
    private static final long TOPOLOGY_ID = 8007;
    private static final long TOPOLOGY_CONTEXT_ID = 182283;
    private static final int CHUNK_SIZE = 100;
    private static final Logger logger = LogManager.getLogger();

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
            .withComponentCluster(ComponentCluster.newBuilder()
                    .withService(ComponentCluster.newService("arangodb")
                            .withMemLimit(2, MetricPrefix.GIGA)
                            .withHealthCheck(new BasicServiceHealthCheck())
                            .logsToLogger(logger))
                    .withService(ComponentCluster.newService("group")
                            .withConfiguration("repositoryHost", ComponentUtils.getDockerHostRoute())
                            .withConfiguration("marketHost", ComponentUtils.getDockerHostRoute())
                            .withMemLimit(4, MetricPrefix.GIGA)
                            .logsToLogger(logger)))
            .withoutStubs()
            .scrapeServicesAndLocalMetricsToInflux("group");

    private SettingPolicyServiceBlockingStub settingPolicyRpcService;

    private SettingPolicyServiceStub settingPolicyRpcServiceAsync;


    @Before
    public void setup() {
        settingPolicyRpcService = SettingPolicyServiceGrpc.newBlockingStub(
                componentTestRule.getCluster().newGrpcChannelBuilder("group")
                        .maxInboundMessageSize(GroupClientConfig.MAX_MSG_SIZE_BYTES)
                        .build());
        settingPolicyRpcServiceAsync = SettingPolicyServiceGrpc.newStub(
            componentTestRule.getCluster().newGrpcChannelBuilder("group")
                .maxInboundMessageSize(GroupClientConfig.MAX_MSG_SIZE_BYTES)
                .build());
    }

    @After
    public void teardown() {
    }

    @Test
    public void testEntitySettings100k() {
        testEntitySettings(100_000);
    }

    @Test
    public void testEntitySettings200k() {
        testEntitySettings(200_000);
    }

    private void testEntitySettings(final long size) {
        final Map<Integer, Long> defaultSettingPolicies = new HashMap<>();
        settingPolicyRpcService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                .setTypeFilter(Type.DEFAULT)
                .build())
            .forEachRemaining(defaultSettingPolicy ->
                defaultSettingPolicies.put(defaultSettingPolicy.getInfo().getEntityType(),
                        defaultSettingPolicy.getId()));

        final Stream<EntitySettings> settingsIt = makeEntitySettings(size, defaultSettingPolicies);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<UploadEntitySettingsResponse> responseObserver =
            new StreamObserver<UploadEntitySettingsResponse>() {

                @Override
                public void onNext(final UploadEntitySettingsResponse value) {

                }

                @Override
                public void onError(final Throwable t) {
                    Status status = Status.fromThrowable(t);
                    logger.error("Failed to upload EntitySettings map to group component"
                        + " for topology {}, due to {}", TOPOLOGY_ID, status);
                    finishLatch.countDown();

                }

                @Override
                public void onCompleted() {
                    logger.warn("Finished uploading EntitySettings map to group component"
                        + " for topology {}.", TOPOLOGY_ID);
                    finishLatch.countDown();
                }
            };
        // Upload entity settings.
        StreamObserver<UploadEntitySettingsRequest> requestObserver =
            settingPolicyRpcServiceAsync.uploadEntitySettings(responseObserver);
        try {
            AtomicInteger counter = new AtomicInteger();
            requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
                .setContext(UploadEntitySettingsRequest.Context.newBuilder()
                    .setTopologyId(TOPOLOGY_ID)
                    .setTopologyContextId(TOPOLOGY_CONTEXT_ID))
                .build());
            Iterators.partition(settingsIt.iterator(), CHUNK_SIZE)
                .forEachRemaining(chunk -> requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
                    .setSettingsChunk(SettingsChunk.newBuilder()
                        .addAllEntitySettings(chunk))
                    .build()));
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();
        try {
            // block until we get a response or an exception occurs.
            finishLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // set interrupt flag
            logger.error("Interrupted while waiting for response", e);
        }

        // Get entity settings for all entities (extreme case).
        settingPolicyRpcService.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setTopologySelection(TopologySelection.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(TOPOLOGY_ID))
                .setSettingFilter(EntitySettingFilter.newBuilder()
                        .addAllEntities(() -> LongStream.range(0, size).iterator()))
                .build());
    }

    @Nonnull
    private Stream<EntitySettings> makeEntitySettings(final long topologySize,
                                          final Map<Integer, Long> defaultSettingPoliciesByType) {
        final List<Integer> types = new ArrayList<>(defaultSettingPoliciesByType.keySet());
        return LongStream.range(0, topologySize)
                .mapToObj(id -> {
                    final Integer entityType = types.get((int)(id % types.size()));
                    return EntitySettings.newBuilder()
                        .setEntityOid(id)
                        .setDefaultSettingPolicyId(defaultSettingPoliciesByType.get(entityType))
                        // TODO (roman, Mar 8 2018): Add user settings at some kind of ration.
                        // We generally expect most entities to not have user settings.
                        .build();
                });
    }
}
