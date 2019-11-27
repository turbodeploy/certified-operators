package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.service.SupplyChainTestUtils;
import com.vmturbo.api.component.external.api.util.GroupExpander.GroupAndMembers;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDisksAspectApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainStatsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainGroupBy;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class SupplyChainFetcherFactoryTest {
    private static final String VM = "VirtualMachine";
    private static final String VV = "VirtualVolume";
    private static final String PM = "PhysicalMachine";
    private static final long LIVE_TOPOLOGY_ID = 1234;

    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final EntitySeverityServiceMole severityServiceBackend =
        spy(new EntitySeverityServiceMole());

    private final SupplyChainTestUtils supplyChainTestUtils = new SupplyChainTestUtils();

    private final EntityAspectMapper entityAspectMapperMock = mock(EntityAspectMapper.class);

    private SupplyChainServiceGrpc.SupplyChainServiceBlockingStub supplyChainRpcService;

    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */

    private final SupplyChainServiceMole supplyChainServiceBackend =
        spy(new SupplyChainServiceMole());

    private RepositoryApi repositoryApiBackend = mock(RepositoryApi.class);
    private GroupExpander groupExpander = mock(GroupExpander.class);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(supplyChainServiceBackend, severityServiceBackend);

    @Before
    public void setup() throws IOException {
        supplyChainRpcService = SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the ActionsService under test
        supplyChainFetcherFactory = new SupplyChainFetcherFactory(
            supplyChainRpcService,
            EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            repositoryApiBackend,
            groupExpander,
            7);
    }

    /**
     * Fetch of an OID that isn't known and isn't the distinguished "Market" returns an empty
     * supplychain.
     *
     * @throws Exception should never happen in this test
     */
    @Test
    public void testEmptyGroupExpansion() throws Exception {
        // arrange
        final ImmutableList<String> supplyChainSeedUuids = ImmutableList.of("x");
        final Set<String> supplyChainSeedUuidSet = Sets.newHashSet(supplyChainSeedUuids);

        // act
        SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .addSeedUuids(supplyChainSeedUuidSet)
                .fetch();

        // assert
        assertThat(result.getSeMap(), notNullValue());
        assertThat(result.getSeMap().size(), equalTo(0));
    }

    /**
     * Verifies that the proper execution order, and return values when detail_type == EntityDetailType.aspects
     *
     * @throws Exception should never happen in this test
     */
    @Test
    public void testGetSupplyChainByUuidsWithAspects() throws Exception {
        // arrange
        final ImmutableList<String> searchUuids = ImmutableList.of("1");
        final String volumeName = "vol1";
        final Set<String> searchUuidSet = Sets.newHashSet(searchUuids);
        final String virtualVolume = "VirtualVolume";

        // Set up to return a VirtualVolume
        SupplychainApiDTO answer = new SupplychainApiDTO();
        Long volumeId = 1L;
        final SupplychainEntryDTO pmSupplyChainEntryDTO = supplyChainTestUtils
                .createSupplyChainEntryDTO(virtualVolume, volumeId);
        supplyChainTestUtils.addHealthSummary(pmSupplyChainEntryDTO, ImmutableMap.of(
                volumeId, "NORMAL"));

        answer.setSeMap(ImmutableMap.of(
                "PhysicalMachine", pmSupplyChainEntryDTO
        ));

        TopologyDTO.TopologyEntityDTO virtualVolumeTopologyEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(volumeId)
                .setDisplayName(volumeName)
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE)
                .build();

        ServiceEntityApiDTO virtualVolumeServiceEntity = new ServiceEntityApiDTO();
        virtualVolumeServiceEntity.setUuid(volumeId.toString());
        virtualVolumeServiceEntity.setDisplayName(volumeName);

        RepositoryApi.MultiEntityRequest volumeRequest = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(virtualVolumeServiceEntity));
        RepositoryApi.MultiEntityRequest volumeFullEntitiesRequest = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList(virtualVolumeTopologyEntity));
        when(repositoryApiBackend.entitiesRequest(Sets.newHashSet(volumeId))).thenReturn(volumeRequest, volumeFullEntitiesRequest);

        VirtualDisksAspectApiDTO virtualDisksAspectApiDTO = new VirtualDisksAspectApiDTO();
        VirtualDiskApiDTO virtualDiskApiDTO = new VirtualDiskApiDTO();
        virtualDiskApiDTO.setDisplayName(volumeName);
        virtualDiskApiDTO.setUuid(volumeName);
        virtualDisksAspectApiDTO.setVirtualDisks(Lists.newArrayList(virtualDiskApiDTO));

        Map<Long, Map<String, EntityAspect>> entityAspectMap = new HashMap<Long, Map<String, EntityAspect>>() {{
            put(1L, new HashMap<String, EntityAspect>() {{
                put(virtualVolume, virtualDisksAspectApiDTO);
            }});
        }};

        final SupplyChainNode virtualVolumes = SupplyChainNode.newBuilder()
                .setEntityType(VV)
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                        .addMemberOids(1L)
                        .build())
                .build();

        when(entityAspectMapperMock.getAspectsByEntities(Lists.newArrayList(virtualVolumeTopologyEntity))).thenReturn(entityAspectMap);

        when(groupExpander.expandUuids(searchUuidSet)).thenReturn(ImmutableSet.of(1L));

        when(supplyChainServiceBackend.getSupplyChain(any()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(SupplyChain.newBuilder()
                            .addSupplyChainNodes(virtualVolumes))
                    .build());

        SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .addSeedUuids(ImmutableList.of(volumeId.toString()))
                .entityDetailType(EntityDetailType.aspects)
                .entityAspectMapper(entityAspectMapperMock)
                .fetch();

        Collection<SupplychainEntryDTO> supplychainEntryDTOs = result.getSeMap().values();
        assertFalse(supplychainEntryDTOs.isEmpty());

        Map<String, ServiceEntityApiDTO> serviceEntityApiDTOMap = supplychainEntryDTOs.iterator().next()
                .getInstances();
        assertFalse(serviceEntityApiDTOMap.isEmpty());

        Map<String, EntityAspect> resultEntityAspectMap = serviceEntityApiDTOMap
                .values().iterator().next()
                .getAspects();
        assertFalse(resultEntityAspectMap.isEmpty());

        Map.Entry<String, EntityAspect> mapEntry = resultEntityAspectMap.entrySet().iterator().next();

        assertEquals(mapEntry.getKey(), virtualVolume);
        assertEquals(mapEntry.getValue().getType(), "VirtualDisksAspectApiDTO");
        assertEquals(((VirtualDisksAspectApiDTO)mapEntry.getValue()).getVirtualDisks().get(0), virtualDiskApiDTO);
    }

    /**
     * Tests health status consistency for all the entities reported as normal.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testHealthStatusAllNormal() throws Exception {
        final String onState = UIEntityState.ACTIVE.apiStr();
        final String offState = UIEntityState.IDLE.apiStr();
        final SupplyChainNode vms = SupplyChainNode.newBuilder()
            .setEntityType(VM)
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                .addMemberOids(1L)
                .addMemberOids(2L)
                .build())
            .putMembersByState(EntityState.POWERED_OFF_VALUE, MemberList.newBuilder()
                .addMemberOids(3L)
                .build())
            .build();

        when(supplyChainServiceBackend.getSupplyChain(any()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(SupplyChain.newBuilder()
                    .addSupplyChainNodes(vms))
                .build());

        final SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .includeHealthSummary(false)
                .fetch();

        assertThat(result.getSeMap().size(), is(1));
        final Map<String, Integer> stateSummary = result.getSeMap().get(VM).getStateSummary();
        assertNotNull(stateSummary);
        assertThat(stateSummary.keySet(), containsInAnyOrder(onState, offState));
        assertThat(stateSummary.get(onState), is(2));
        assertThat(stateSummary.get(offState), is(1));
    }

    /**
     * Tests proper extraction of entity ids from a correct result.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testFetchIds() throws Exception {
        final SupplyChainNode supplyChainNode1 =
            SupplyChainNode.newBuilder()
                .putMembersByState(
                    EntityState.POWERED_ON_VALUE,
                    MemberList.newBuilder().addMemberOids(1L).addMemberOids(2L).build())
                .putMembersByState(
                    EntityState.POWERED_OFF_VALUE,
                    MemberList.newBuilder().addMemberOids(3L).build())
                .build();
        final SupplyChainNode supplyChainNode2 =
                SupplyChainNode.newBuilder()
                    .putMembersByState(
                        EntityState.POWERED_ON_VALUE,
                        MemberList.newBuilder().addMemberOids(1L).addMemberOids(4L).build())
                    .putMembersByState(
                        EntityState.POWERED_OFF_VALUE,
                        MemberList.newBuilder().addMemberOids(4L).addMemberOids(5L).build())
                    .build();

        when(supplyChainServiceBackend.getSupplyChain(any()))
            .thenReturn(
                GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(
                        SupplyChain.newBuilder()
                            .addSupplyChainNodes(supplyChainNode1)
                            .addSupplyChainNodes(supplyChainNode2)
                            .build())
                    .build());

        Assert.assertEquals(
            LongStream.range(1L, 6L).boxed().collect(Collectors.toSet()),
            supplyChainFetcherFactory.newNodeFetcher().fetchEntityIds());
    }

    @Test
    public void testFetchStats() throws OperationFailedException {
        final String seed = "100";

        when(groupExpander.expandUuids(Collections.singleton(seed)))
            .thenReturn(Collections.singleton(Long.parseLong(seed)));

        final SupplyChainStat stat = SupplyChainStat.newBuilder()
            .setNumEntities(100)
            .build();

        when(supplyChainServiceBackend.getSupplyChainStats(any()))
            .thenReturn(GetSupplyChainStatsResponse.newBuilder()
                .addStats(stat)
                .build());

        assertThat(supplyChainFetcherFactory.newNodeFetcher()
            .addSeedUuid(seed)
            .apiEnvironmentType(EnvironmentType.CLOUD)
            .entityTypes(Collections.singletonList(UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .fetchStats(Collections.singletonList(SupplyChainGroupBy.BUSINESS_ACCOUNT_ID)), containsInAnyOrder(stat));
    }

    @Test
    public void testSupplyChainNodeFetcher() throws Exception {
        final SupplyChainNode vms = SupplyChainNode.newBuilder()
                .setEntityType(VM)
                .putMembersByState(EntityState.POWERED_ON.getNumber(),
                        MemberList.newBuilder().addMemberOids(1L).build())
                .build();
        when(supplyChainServiceBackend.getSupplyChain(GetSupplyChainRequest.newBuilder()
                .setScope(SupplyChainScope.newBuilder()
                    .addEntityTypesToInclude(VM))
                .setEnforceUserScope(true)
                .build()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(SupplyChain.newBuilder()
                    .addSupplyChainNodes(vms))
                .build());
        final Map<String, SupplyChainNode> nodes = supplyChainFetcherFactory.newNodeFetcher()
                .entityTypes(Collections.singletonList(VM))
                .fetch();
        assertThat(nodes.size(), is(1));
        assertThat(nodes.get(VM), is(vms));
        assertThat(nodes.get(VM).containsMembersByState(EntityState.POWERED_ON.getNumber()), is(true));
    }

    /**
     * Tests fetching a group without members.
     */
    @Test
    public void testEmptyGroupNodeFetcher() throws Exception {
        GroupAndMembers groupAndMembers = mock(GroupAndMembers.class);
        when(groupAndMembers.group()).thenReturn(Grouping.newBuilder()
                        .setId(1)
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.REGULAR)
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                                        .addMembersByType(StaticMembersByType
                                                                        .newBuilder()
                                                                        .setType(MemberType
                                                                                        .newBuilder()
                                                                                        .setEntity(UIEntityType.VIRTUAL_MACHINE
                                                                                                        .typeNumber()))))
                                        ).build());
        when(groupAndMembers.members()).thenReturn(Arrays.asList());
        when(groupExpander.getGroupWithMembers("group1")).thenReturn(Optional.of(groupAndMembers));

        final SupplyChainNode vms = SupplyChainNode.newBuilder()
            .setEntityType(VM)
            .putMembersByState(EntityState.POWERED_ON.getNumber(),
                MemberList.newBuilder().addMemberOids(1L).build())
            .build();
        when(supplyChainServiceBackend.getSupplyChain(GetSupplyChainRequest.newBuilder()
            .setScope(SupplyChainScope.newBuilder()
                .addEntityTypesToInclude(VM))
            .setEnforceUserScope(true)
            .build()))
            .thenReturn(GetSupplyChainResponse.newBuilder()
                .setSupplyChain(SupplyChain.newBuilder()
                    .addSupplyChainNodes(vms))
                .build());
        final Map<String, SupplyChainNode> nodes =
            supplyChainFetcherFactory.newNodeFetcher().addSeedUuids(Arrays.asList("group1"))
            .entityTypes(Collections.singletonList(VM))
            .fetch();
        assertThat(nodes.size(), is(0));
    }

    /**
     * Tests fetching the supply chain and filter by environment type, the members in supply chain
     * node should only contain entities matching the requested environment type.
     *
     * @throws Exception exception thrown in test, should not happen
     */
    @Test
    public void testFilterByEnvironmentType() throws Exception {
        final String groupId = "1";
        final long vmId1 = 111L;
        final long vmId2 = 112L;
        GroupAndMembers groupAndMembers = mock(GroupAndMembers.class);
        when(groupAndMembers.group()).thenReturn(Grouping.newBuilder()
                .setId(1)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.REGULAR)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType
                                        .newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                                        .addMembers(vmId1)
                                        .addMembers(vmId2))))
                .addExpectedTypes(MemberType.newBuilder()
                        .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                .build());
        when(groupAndMembers.members()).thenReturn(Arrays.asList(vmId1, vmId2));
        when(groupAndMembers.entities()).thenReturn(Arrays.asList(vmId1, vmId2));
        when(groupExpander.getGroupWithMembers(groupId)).thenReturn(Optional.of(groupAndMembers));
        when(groupExpander.expandUuidToTypeToEntitiesMap(eq(1L))).thenReturn(
            ImmutableMap.of(UIEntityType.VIRTUAL_MACHINE, ImmutableSet.of(vmId1, vmId2)));

        RepositoryApi.MultiEntityRequest req1and2 = ApiTestUtils.mockMultiMinEntityReq(
                Lists.newArrayList(
                        MinimalEntity.newBuilder()
                                .setOid(vmId1)
                                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                                .build(),
                        MinimalEntity.newBuilder()
                                .setOid(vmId2)
                                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                                .build()));
        when(repositoryApiBackend.entitiesRequest(ImmutableSet.of(vmId1, vmId2))).thenReturn(req1and2);
        // test filter by environment type: cloud
        Map<String, SupplyChainNode> nodes = supplyChainFetcherFactory.newNodeFetcher()
                .addSeedUuids(Collections.singletonList(groupId))
                .entityTypes(Collections.singletonList(VM))
                .apiEnvironmentType(com.vmturbo.api.enums.EnvironmentType.CLOUD)
                .fetch();
        assertThat(nodes.size(), is(1));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodes.values().iterator().next()),
                containsInAnyOrder(vmId1));
        // test filter by environment type: onprem
        nodes = supplyChainFetcherFactory.newNodeFetcher()
                .addSeedUuids(Collections.singletonList(groupId))
                .entityTypes(Collections.singletonList(VM))
                .apiEnvironmentType(com.vmturbo.api.enums.EnvironmentType.ONPREM)
                .fetch();
        assertThat(nodes.size(), is(1));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodes.values().iterator().next()),
                containsInAnyOrder(vmId2));
        // test filter by environment type: hybrid
        nodes = supplyChainFetcherFactory.newNodeFetcher()
                .addSeedUuids(Collections.singletonList(groupId))
                .entityTypes(Collections.singletonList(VM))
                .apiEnvironmentType(com.vmturbo.api.enums.EnvironmentType.HYBRID)
                .fetch();
        assertThat(nodes.size(), is(1));
        assertThat(RepositoryDTOUtil.getAllMemberOids(nodes.values().iterator().next()),
                containsInAnyOrder(vmId1, vmId2));
    }

    /**
     * Tests that, when {@link SupplyChainNodeFetcherBuilder#expandScope} is called,
     * a supply chain fetcher will be created, that the two requirements will be passed
     * correctly to it, and that the method {@link SupplyChainNodeFetcherBuilder#fetch}
     * will be called.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testExpandScopeCheckServiceCalls() throws Exception {
        // input
        final Set<Long> seedsIds = ImmutableSet.of(1L, 2L, 3L);
        final Set<String> seedIdStrings = ImmutableSet.of("1", "2", "3");
        final Set<Long> result = Collections.singleton(150L);
        final List<String> relatedEntityTypes = Collections.singletonList(VM);

        // mock underlying supply chain call
        when(groupExpander.expandUuids(eq(seedIdStrings))).thenReturn(seedsIds);
        when(supplyChainServiceBackend.getSupplyChain(
                GetSupplyChainRequest.newBuilder()
                    .setScope(SupplyChainScope.newBuilder()
                        .addAllEntityTypesToInclude(relatedEntityTypes)
                        .addAllStartingEntityOid(seedsIds))
                    .setEnforceUserScope(true)
                    .build()))
            .thenReturn(
                GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(SupplyChain.newBuilder()
                    .addSupplyChainNodes(
                        SupplyChainNode.newBuilder()
                            .setEntityType(VM)
                            .putMembersByState(10, MemberList.newBuilder().addMemberOids(150L).build())))
                    .build());

        // call service
        Assert.assertEquals(result, supplyChainFetcherFactory.expandScope(seedsIds, relatedEntityTypes));
    }

    /**
     * Tests health status consistency for different status of entities.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testHealthStatusWithCritical() throws Exception {
        // arrange
        final SupplyChainNode vms = SupplyChainNode.newBuilder()
            .setEntityType(VM)
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                    .addMemberOids(1L)
                    .addMemberOids(2L)
                    .build())
            .build();
        final SupplyChainNode hosts = SupplyChainNode.newBuilder()
            .setEntityType(PM)
            .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                    .addMemberOids(5L)
                    .build())
            .build();
        when(supplyChainServiceBackend.getSupplyChain(any()))
                .thenReturn(GetSupplyChainResponse.newBuilder()
                    .setSupplyChain(SupplyChain.newBuilder()
                        .addSupplyChainNodes(vms)
                        .addSupplyChainNodes(hosts))
                    .build());

        when(severityServiceBackend.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(LIVE_TOPOLOGY_ID)
                .addEntityIds(1L)
                .addEntityIds(2L)
                .build()))
            .thenReturn(EntitySeveritiesResponse.newBuilder()
                    .addAllEntitySeverity(
                            Arrays.asList(newSeverity(1L, Severity.CRITICAL),
                                    newSeverity(2L, null)))
                    .build());
        when(severityServiceBackend.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(LIVE_TOPOLOGY_ID)
                .addEntityIds(5L)
                .build()))
            .thenReturn(EntitySeveritiesResponse.newBuilder()
                    .addAllEntitySeverity(Collections.singletonList(newSeverity(5L, Severity.MAJOR)))
                    .build());

        RepositoryApi.MultiEntityRequest req1and2 = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(
            createServiceEntityApiDTO(1L, VM, Severity.CRITICAL),
            createServiceEntityApiDTO(2L, VM, null)));
        when(repositoryApiBackend.entitiesRequest(ImmutableSet.of(1L, 2L)))
            .thenReturn(req1and2);

        RepositoryApi.MultiEntityRequest req5 = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(
            createServiceEntityApiDTO(5L, PM, Severity.MAJOR)));
        when(repositoryApiBackend.entitiesRequest(ImmutableSet.of(5L)))
            .thenReturn(req5);

        // act
        final SupplychainApiDTO result = supplyChainFetcherFactory.newApiDtoFetcher()
                .topologyContextId(LIVE_TOPOLOGY_ID)
                .addSeedUuid("Market")
                .includeHealthSummary(true)
                .entityDetailType(EntityDetailType.entity)
                .fetch();

        // assert
        Assert.assertEquals(2, result.getSeMap().size());
        Assert.assertEquals(RepositoryDTOUtil.getMemberCount(vms), getObjectsCountInHealth(result, VM));
        Assert.assertEquals(RepositoryDTOUtil.getMemberCount(hosts), getObjectsCountInHealth(result, PM));

        Assert.assertEquals(1, getSeveritySize(result, VM, Severity.CRITICAL));
        Assert.assertEquals(1, getSeveritySize(result, VM, Severity.NORMAL));

        Assert.assertEquals(1, getSeveritySize(result, PM, Severity.MAJOR));

        // test that the SE's returned are populated with the severity info
        Assert.assertEquals(Severity.CRITICAL.name(),
                result.getSeMap().get(VM).getInstances().get("1").getSeverity());
        Assert.assertEquals(Severity.NORMAL.name(), // by default
                result.getSeMap().get(VM).getInstances().get("2").getSeverity());
        Assert.assertEquals(Severity.MAJOR.name(),
                result.getSeMap().get(PM).getInstances().get("5").getSeverity());
    }

    private int getSeveritySize(@Nonnull final SupplychainApiDTO src, @Nonnull String objType,
                                @Nonnull final Severity severity) {
        final String severityStr = ActionDTOUtil.getSeverityName(severity);
        return src.getSeMap().get(objType).getHealthSummary().get(severityStr);
    }

    private int getObjectsCountInHealth(SupplychainApiDTO src, String objType) {
        return src.getSeMap()
            .get(objType)
            .getHealthSummary()
            .values()
            .stream()
            .mapToInt(Integer::intValue)
            .sum();
    }

    private static ServiceEntityApiDTO createServiceEntityApiDTO(long id, String entityType,
                                                                 @Nullable Severity severity) {
        ServiceEntityApiDTO answer = new ServiceEntityApiDTO();
        answer.setUuid(Long.toString(id));
        answer.setClassName(entityType);
        if (severity != null) {
            answer.setSeverity(severity.name());
        }
        return answer;
    }

    @Nonnull
    private static EntitySeverity newSeverity(final long entityId, @Nullable Severity severity) {
        final EntitySeverity.Builder builder = EntitySeverity.newBuilder()
                .setEntityId(entityId);
        if (severity != null) {
            builder.setSeverity(severity);
        }
        return builder.build();
    }
}
