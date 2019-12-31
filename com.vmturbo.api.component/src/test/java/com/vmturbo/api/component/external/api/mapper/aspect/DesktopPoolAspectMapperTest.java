package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.dto.entityaspect.DesktopPoolEntityAspectApiDTO;
import com.vmturbo.api.enums.DesktopPoolAssignmentType;
import com.vmturbo.api.enums.DesktopPoolCloneType;
import com.vmturbo.api.enums.DesktopPoolProvisionType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;

/**
 * Unit test for {@link DesktopPoolAspectMapper}.
 */
public class DesktopPoolAspectMapperTest extends BaseAspectMapperTest {
    private GroupServiceMole groupServiceMole = spy(new GroupServiceMole());
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(groupServiceMole);

    private static final long VIRTUAL_MACHINE_ID = 1000;
    private static final long DESKTOP_POOL_OID = 1001;
    private static final long VM_REFERENCE_ID = 1002;
    private static final long PHYSICAL_MACHINE_OID = 1003;
    private static final long CLUSTER_ID = 1004;
    private static final String CLUSTER_NAME = "vsphere-dc01-DC01\\vsphere-dc01-Clus01";
    private static final String SNAPSHOT = "/Clone Snapshot";
    private static final String VENDOR_ID = "SITE01-POD01-POOL01-FULL-SMALL-1VCPU-2GB";

    private final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private GroupServiceBlockingStub groupServiceBlockingStub;
    private DesktopPoolAspectMapper desktopPoolAspectMapper;
    private TopologyEntityDTO desktopPool;

    /**
     * Objects initialization necessary for a unit test.
     */
    @Before
    public void setUp() {
        groupServiceBlockingStub = GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        desktopPoolAspectMapper =
                new DesktopPoolAspectMapper(repositoryApi, groupServiceBlockingStub);
        final TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
                .setDesktopPool(DesktopPoolInfo.newBuilder()
                        .setVmWithSnapshot(VmWithSnapshot.newBuilder()
                                .setVmReferenceId(VM_REFERENCE_ID)
                                .setSnapshot(SNAPSHOT)
                                .build())
                        .setAssignmentType(DesktopPoolData.DesktopPoolAssignmentType.DYNAMIC)
                        .setCloneType(DesktopPoolData.DesktopPoolCloneType.LINKED)
                        .setProvisionType(DesktopPoolData.DesktopPoolProvisionType.ON_DEMAND)
                        .build())
                .build();
        final SearchRequest searchRequest = ApiTestUtils.mockSearchMinReq(Collections.singletonList(
                MinimalEntity.newBuilder()
                        .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
                        .setOid(PHYSICAL_MACHINE_OID)
                        .build()));
        final Grouping cluster = Grouping.newBuilder()
                .setId(CLUSTER_ID)
                .setDefinition(GroupDefinition.newBuilder()
                                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                                .setDisplayName(CLUSTER_NAME))
                .build();
        Mockito.when(groupServiceMole.getGroupsForEntities(GetGroupsForEntitiesRequest.newBuilder()
                .addEntityId(PHYSICAL_MACHINE_OID)
                .addGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                .setLoadGroupObjects(true)
                .build()))
                .thenReturn(GetGroupsForEntitiesResponse.newBuilder()
                        .putEntityGroup(PHYSICAL_MACHINE_OID,
                                Groupings.newBuilder().addGroupId(CLUSTER_ID).build())
                        .addGroups(cluster)
                        .build());
        Mockito.when(repositoryApi.newSearchRequest(Mockito.any())).thenReturn(searchRequest);
        desktopPool = topologyEntityDTOBuilder(EntityType.DESKTOP_POOL,
                typeSpecificInfo).putEntityPropertyMap(SDKUtil.VENDOR_ID, VENDOR_ID).build();
    }

    /**
     * Test map entity to aspect. Сase for desktop pool.
     */
    @Test
    public void testMapEntityToAspectDesktopPool() {
        // act
        final DesktopPoolEntityAspectApiDTO aspect =
                (DesktopPoolEntityAspectApiDTO)desktopPoolAspectMapper.mapEntityToAspect(
                        desktopPool);
        // assert
        checkAspect(aspect);
    }

    /**
     * Test map entity to aspect. Сase for virtual machine.
     */
    @Test
    public void testMapEntityToAspectVirtualMachine() {
        final SingleEntityRequest desktopPoolRequest =
                ApiTestUtils.mockSingleEntityRequest(desktopPool);
        Mockito.when(repositoryApi.entityRequest(DESKTOP_POOL_OID)).thenReturn(desktopPoolRequest);

        final List<CommodityBoughtDTO> commodityBoughtDTOList = ImmutableList.of(
                CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.CPU_ALLOCATION_VALUE))
                        .build());
        final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders =
                ImmutableList.of(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.DESKTOP_POOL_VALUE)
                        .setProviderId(DESKTOP_POOL_OID)
                        .addAllCommodityBought(commodityBoughtDTOList)
                        .build());
        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setOid(VIRTUAL_MACHINE_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addAllCommoditiesBoughtFromProviders(commoditiesBoughtFromProviders)
                .build();
        // act
        final DesktopPoolEntityAspectApiDTO aspect =
                (DesktopPoolEntityAspectApiDTO)desktopPoolAspectMapper.mapEntityToAspect(vm);
        // assert
        checkAspect(aspect);
    }

    private void checkAspect(DesktopPoolEntityAspectApiDTO aspect) {
        Assert.assertEquals(DesktopPoolAssignmentType.DYNAMIC, aspect.getAssignmentType());
        Assert.assertEquals(DesktopPoolProvisionType.ON_DEMAND, aspect.getProvisionType());
        Assert.assertEquals(DesktopPoolCloneType.LINKED, aspect.getCloneType());
        Assert.assertEquals(String.valueOf(VM_REFERENCE_ID), aspect.getMasterVirtualMachineUuid());
        Assert.assertEquals(VENDOR_ID, aspect.getVendorId());
        Assert.assertEquals(CLUSTER_NAME, aspect.getvCenterClusterName());
        Assert.assertEquals(SNAPSHOT, aspect.getMasterVirtualMachineSnapshot());
    }

    /**
     * Compliance check
     * {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolAssignmentType}
     * to {@link com.vmturbo.api.enums.DesktopPoolAssignmentType},
     * {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolProvisionType}
     * to {@link com.vmturbo.api.enums.DesktopPoolProvisionType},
     * {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolCloneType}
     * to {@link com.vmturbo.api.enums.DesktopPoolCloneType}.
     */
    @Test
    public void testDesktopPoolEnums() {
        Arrays.stream(DesktopPoolData.DesktopPoolAssignmentType.values())
                .forEach(v -> DesktopPoolAssignmentType.valueOf(v.name()));
        Arrays.stream(DesktopPoolData.DesktopPoolProvisionType.values())
                .forEach(v -> DesktopPoolProvisionType.valueOf(v.name()));
        Arrays.stream(DesktopPoolData.DesktopPoolCloneType.values())
                .forEach(v -> DesktopPoolCloneType.valueOf(v.name()));
    }
}
