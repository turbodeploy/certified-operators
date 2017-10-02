package com.vmturbo.topology.processor.rest;


import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetHostInfoRequest;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetHostInfoResponse;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.HostInfo;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PhysicalMachineData;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityRpcService;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;


public class EntityRpcServiceTest {
    private TargetStore targetStore = Mockito.mock(TargetStore.class);
    private EntityStore entityStore = Mockito.mock(EntityStore.class);

    private EntityServiceGrpc.EntityServiceBlockingStub entityServiceClient;
    private GrpcTestServer server;

    private static final long TARGET_ID = 1234L;
    private final PhysicalMachineData fastHostProperties = PhysicalMachineData.newBuilder()
        .setCpuCoreMhz(4000)
        .setNumCpuCores(16)
        .setNumCpuSockets(8)
        .setNumCpuThreads(32)
        .build();

    private final PhysicalMachineData slowHostProperties = PhysicalMachineData.newBuilder()
        .setCpuCoreMhz(2000)
        .setNumCpuCores(8)
        .setNumCpuSockets(4)
        .setNumCpuThreads(16)
        .build();

    @Before
    public void setup() throws Exception {
        final EntityRpcService entityRpcServiceBackend = new EntityRpcService(entityStore, targetStore);
        server = GrpcTestServer.withServices(entityRpcServiceBackend);
        entityServiceClient = EntityServiceGrpc.newBlockingStub(server.getChannel());

        Mockito.when(entityStore.getEntity(Mockito.anyLong())).thenReturn(Optional.empty());
        addEntity(1, ImmutableMap.of(1L, 1L));
        addEntity(2, ImmutableMap.of(1L, 1L));
    }

    @After
    public void teardown() {
        server.close();
    }

    /**
     * Test getting multiple entities, all of which should be present.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetMultiple() throws Exception {
        EntityInfoOuterClass.GetEntitiesInfoRequest.Builder requestBuilder =
                EntityInfoOuterClass.GetEntitiesInfoRequest.newBuilder();
        requestBuilder.addEntityIds(1);
        requestBuilder.addEntityIds(2);
        EntityInfoOuterClass.GetEntitiesInfoRequest request = requestBuilder.build();
        Iterator<EntityInfoOuterClass.EntityInfo> response = entityServiceClient.getEntitiesInfo(request);

        final List<EntityInfoOuterClass.EntityInfo> entities = Lists.newArrayList(response);
        assertEquals(2, entities.size());
        final Map<Long, EntityInfoOuterClass.EntityInfo> entityMap = entities.stream()
                .collect(Collectors.toMap(EntityInfoOuterClass.EntityInfo::getEntityId, Function.identity()));
        Assert.assertTrue(entityMap.containsKey(1L));
        Assert.assertTrue(entityMap.containsKey(2L));

        for (final EntityInfoOuterClass.EntityInfo entityInfo : entities) {
            assertEquals(1, entityInfo.getTargetIdToProbeIdMap().get(1L).longValue());
        }
    }

    /**
     * Test getting multiple entities, some of which are not found.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetSomeMissing() throws Exception {
        EntityInfoOuterClass.GetEntitiesInfoRequest.Builder requestBuilder =
                EntityInfoOuterClass.GetEntitiesInfoRequest.newBuilder();
        requestBuilder.addEntityIds(1);
        requestBuilder.addEntityIds(3);
        EntityInfoOuterClass.GetEntitiesInfoRequest request = requestBuilder.build();

        Iterator<EntityInfoOuterClass.EntityInfo> response = entityServiceClient.getEntitiesInfo(request);
        final List<EntityInfoOuterClass.EntityInfo> entities = Lists.newArrayList(response);
        assertEquals(1, entities.size());
        final Map<Long, EntityInfoOuterClass.EntityInfo> entityMap = entities.stream()
                .collect(Collectors.toMap(EntityInfoOuterClass.EntityInfo::getEntityId, Function.identity()));
        Assert.assertTrue(entityMap.containsKey(1L));
        Assert.assertFalse(entityMap.containsKey(3L));
    }

    @Test
    public void testGetHostsInfo() throws Exception {
        givenVmHostWithProperties(4, 14, fastHostProperties);

        GetHostInfoRequest hostInfoRequest = GetHostInfoRequest.newBuilder()
            .addVirtualMachineIds(4)
            .build();

        final Iterable<GetHostInfoResponse> responses = () -> entityServiceClient.getHostsInfo(hostInfoRequest);
        final Map<Long, Optional<HostInfo>> hostInfo = StreamSupport.stream(responses.spliterator(), false)
            .collect(Collectors.toMap(
                    GetHostInfoResponse::getVirtualMachineId,
                    response -> response.hasHostInfo() ? Optional.of(response.getHostInfo()) : Optional.empty()
                ));

        assertHostWithIdAndProperties(hostInfo.get(4L), 14, fastHostProperties);
    }

    @Test
    public void testGetMultipleHostsInfo() throws Exception {
        givenVmHostWithProperties(4, 14, fastHostProperties);
        givenVmHostWithProperties(5, 15, slowHostProperties);

        GetHostInfoRequest hostInfoRequest = GetHostInfoRequest.newBuilder()
            .addAllVirtualMachineIds(Arrays.asList(4L, 5L))
            .build();

        final Iterable<GetHostInfoResponse> responses = () -> entityServiceClient.getHostsInfo(hostInfoRequest);
        final Map<Long, Optional<HostInfo>> hostInfo = StreamSupport.stream(responses.spliterator(), false)
            .collect(Collectors.toMap(
                GetHostInfoResponse::getVirtualMachineId,
                response -> response.hasHostInfo() ? Optional.of(response.getHostInfo()) : Optional.empty()
            ));

        assertHostWithIdAndProperties(hostInfo.get(4L), 14, fastHostProperties);
        assertHostWithIdAndProperties(hostInfo.get(5L), 15, slowHostProperties);
    }

    @Test
    public void testGetHostsInfoSomeMissing() throws Exception {
        givenVmHostWithProperties(4, 14, fastHostProperties);
        givenVmHostWithProperties(5, 15, slowHostProperties);

        GetHostInfoRequest hostInfoRequest = GetHostInfoRequest.newBuilder()
            .addAllVirtualMachineIds(Arrays.asList(4L, 5L, 6L))
            .build();

        final Iterable<GetHostInfoResponse> responses = () -> entityServiceClient.getHostsInfo(hostInfoRequest);
        final Map<Long, Optional<HostInfo>> hostInfo = StreamSupport.stream(responses.spliterator(), false)
            .collect(Collectors.toMap(
                GetHostInfoResponse::getVirtualMachineId,
                response -> response.hasHostInfo() ? Optional.of(response.getHostInfo()) : Optional.empty()
            ));

        assertHostWithIdAndProperties(hostInfo.get(4L), 14, fastHostProperties);
        assertHostWithIdAndProperties(hostInfo.get(5L), 15, slowHostProperties);
        assertEquals(Optional.empty(), hostInfo.get(6L));
    }

    private void addEntity(final long entityId,
                           final Map<Long, Long> targetToProbeMap) {
        final Entity entity = new Entity(entityId);
        targetToProbeMap.entrySet().forEach(targetToProbe -> {
            final Target target = Mockito.mock(Target.class);
            Mockito.when(target.getId()).thenReturn(targetToProbe.getKey());
            Mockito.when(target.getProbeId()).thenReturn(targetToProbe.getValue());
            entity.addTargetInfo(targetToProbe.getKey(), CommonDTO.EntityDTO.getDefaultInstance());
            Mockito.when(targetStore.getTarget(Mockito.eq(targetToProbe.getKey()))).thenReturn(Optional.of(target));
        });
        Mockito.when(entityStore.getEntity(Mockito.eq(entityId))).thenReturn(Optional.of(entity));
    }

    private void givenVmHostWithProperties(final long vmId,
                                           final long hostId,
                                           @Nonnull final PhysicalMachineData hostProperties) {
        Preconditions.checkArgument(vmId != hostId, "VM and Host must have different IDs!");
        final Entity vm = new Entity(vmId);
        final Entity host = new Entity(hostId);

        final EntityDTO vmDto = EntityDTO.newBuilder()
            .setId("vm-" + vmId)
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .build();
        final EntityDTO hostDto = EntityDTO.newBuilder()
            .setId("pm-" + hostId)
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setPhysicalMachineData(hostProperties)
            .build();

        vm.addTargetInfo(TARGET_ID, vmDto);
        vm.setHostedBy(TARGET_ID, hostId);

        host.addTargetInfo(TARGET_ID, hostDto);

        Mockito.when(entityStore.getEntity(Mockito.eq(vmId))).thenReturn(Optional.of(vm));
        Mockito.when(entityStore.getEntity(Mockito.eq(hostId))).thenReturn(Optional.of(host));
    }

    private void assertHostWithIdAndProperties(@Nonnull final Optional<HostInfo> hostInfo,
                                               final long expectedHostId,
                                               @Nonnull final PhysicalMachineData expectedPmProperties) {
        Objects.requireNonNull(hostInfo);
        assertTrue(hostInfo.isPresent());
        final HostInfo info = hostInfo.get();
        assertEquals(expectedHostId, info.getHostId());
        assertEquals(expectedPmProperties.getCpuCoreMhz(), info.getCpuCoreMhz());
        assertEquals(expectedPmProperties.getNumCpuCores(), info.getNumCpuCores());
        assertEquals(expectedPmProperties.getNumCpuSockets(), info.getNumCpuSockets());
        assertEquals(expectedPmProperties.getNumCpuThreads(), info.getNumCpuThreads());
    }
}
