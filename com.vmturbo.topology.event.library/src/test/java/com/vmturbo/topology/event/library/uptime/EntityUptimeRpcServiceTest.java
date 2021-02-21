package com.vmturbo.topology.event.library.uptime;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.EntityUptime.CloudScopeFilter;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;
import com.vmturbo.common.protobuf.cost.EntityUptime.ForceFullUptimeCalculationRequest;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeByFilterRequest;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeByFilterResponse;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeRequest;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeResponse;
import com.vmturbo.common.protobuf.cost.EntityUptimeServiceGrpc;
import com.vmturbo.common.protobuf.cost.EntityUptimeServiceGrpc.EntityUptimeServiceBlockingStub;

public class EntityUptimeRpcServiceTest {

    private EntityUptimeStore entityUptimeStore = mock(EntityUptimeStore.class);

    private EntityUptimeManager entityUptimeManager = mock(EntityUptimeManager.class);

    /**
     * Setting the grpc test server.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private EntityUptimeServiceBlockingStub uptimeClient;

    private EntityUptimeRpcService entityUptimeRpcService;

    @Before
    public void setup() throws IOException {
        entityUptimeRpcService = new EntityUptimeRpcService(entityUptimeStore, entityUptimeManager);

        // Generate a unique in-process server name.
        final String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(
                InProcessServerBuilder
                        .forName(serverName)
                        .directExecutor()
                        .addService(entityUptimeRpcService)
                        .build()
                        .start());

        // Create a client channel and register for automatic graceful shutdown.
        final ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder
                        .forName(serverName)
                        .directExecutor()
                        .build());

        uptimeClient = EntityUptimeServiceGrpc.newBlockingStub(channel);
    }

    @Test
    public void testForceFullUptimeCalculation() {

        // invoke SUT
        final ForceFullUptimeCalculationRequest request = ForceFullUptimeCalculationRequest.getDefaultInstance();
        uptimeClient.forceFullUptimeCalculation(request);

        // Verify
        verify(entityUptimeManager).updateUptimeForTopology(eq(true));
    }


    @Test
    public void testGetEntityUptime() {

        when(entityUptimeStore.getEntityUptime(anyLong())).thenReturn(Optional.of(EntityUptime.UNKNOWN_DEFAULT_TO_ALWAYS_ON));

        // Invoke SUT
        final GetEntityUptimeRequest request = GetEntityUptimeRequest.newBuilder()
                .setEntityOid(123L)
                .build();
        final GetEntityUptimeResponse response = uptimeClient.getEntityUptime(request);

        // Verify
        verify(entityUptimeStore).getEntityUptime(eq(123L));

        // Asserts
        final EntityUptimeDTO expectedUptimeDTO = EntityUptimeDTO.newBuilder()
                .setUptimeDurationMs(0)
                .setTotalDurationMs(0)
                .setUptimePercentage(100.0)
                .build();

        assertThat(response.getEntityUptime(), equalTo(expectedUptimeDTO));
    }

    @Test
    public void testGetEntityUptimeByFilter() {

        // setup entityUptimeStore
        final Map<Long, EntityUptime> entityUptimeMap = ImmutableMap.of(
                1L, EntityUptime.builder()
                        .uptime(Duration.ofSeconds(2))
                        .totalTime(Duration.ofSeconds(4))
                        .uptimePercentage(50.0)
                        .build(),
                2L, EntityUptime.builder()
                        .uptime(Duration.ofSeconds(9))
                        .totalTime(Duration.ofSeconds(12))
                        .uptimePercentage(75.0)
                        .build());
        when(entityUptimeStore.getUptimeByFilter(any())).thenReturn(entityUptimeMap);
        when(entityUptimeStore.getDefaultUptime()).thenReturn(Optional.of(EntityUptime.UNKNOWN_DEFAULT_TO_ALWAYS_ON));

        // invoke SUT
        final CloudScopeFilter filter = CloudScopeFilter.newBuilder()
                .addAccountOid(2)
                .addAccountOid(3)
                .addRegionOid(4)
                .build();
        final GetEntityUptimeByFilterRequest request = GetEntityUptimeByFilterRequest.newBuilder()
                .setFilter(filter)
                .build();
        final GetEntityUptimeByFilterResponse response = uptimeClient.getEntityUptimeByFilter(request);

        // Asserts
        final Map<Long, EntityUptimeDTO> actualUptimeMap = response.getEntityUptimeByOidMap();
        assertThat(actualUptimeMap.entrySet(), hasSize(2));
        assertThat(actualUptimeMap.get(1L), equalTo(entityUptimeMap.get(1L).toProtobuf()));
        assertThat(actualUptimeMap.get(2L), equalTo(entityUptimeMap.get(2L).toProtobuf()));
    }
}
