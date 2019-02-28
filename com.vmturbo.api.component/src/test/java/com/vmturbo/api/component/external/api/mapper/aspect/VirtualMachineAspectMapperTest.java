package com.vmturbo.api.component.external.api.mapper.aspect;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

public class VirtualMachineAspectMapperTest extends BaseAspectMapperTest {

    public static final long CONNECTED_PROCESSOR_POOL_ID = 123L;
    public static final long CONNECTED_NETWORK_ID_1 = 2333L;
    public static final long CONNECTED_NETWORK_ID_2 = 666L;
    public static final String CONNECTED_PROCESSOR_POOL_NAME = "processor pool";
    public static final String CONNECTED_NETWORK_NAME_1 = "network 1";
    public static final String CONNECTED_NETWORK_NAME_2 = "network 2";
    public static final List<Long> CONNECTED_ENTITY_ID_LIST = ImmutableList.of(
        CONNECTED_PROCESSOR_POOL_ID, CONNECTED_NETWORK_ID_1, CONNECTED_NETWORK_ID_2);
    public static final List<String> IP_ADDRESSES = ImmutableList.of("1.2.3.4", "5.6.7.8");

    SearchServiceBlockingStub searchRpc;

    SearchServiceMole searchServiceSpy = Mockito.spy(new SearchServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(searchServiceSpy);

    @Before
    public void init() throws Exception {
        searchRpc = SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }
    @Test
    public void testMapEntityToAspect() {
        final TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                .setGuestOsInfo(OS.newBuilder()
                    .setGuestOsType(OSType.LINUX)
                    .setGuestOsName(OSType.LINUX.name()))
                .setNumCpus(4)
                .addIpAddresses(IpAddress.newBuilder().setIpAddress(IP_ADDRESSES.get(0)))
                .addIpAddresses(IpAddress.newBuilder().setIpAddress(IP_ADDRESSES.get(1)))
            )
            .build();
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
            EntityType.VIRTUAL_MACHINE, typeSpecificInfo);

        VirtualMachineAspectMapper testMapper = new VirtualMachineAspectMapper(searchRpc);
        // act
        final EntityAspect resultAspect = testMapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertTrue(resultAspect instanceof VMEntityAspectApiDTO);
        final VMEntityAspectApiDTO vmAspect = (VMEntityAspectApiDTO) resultAspect;
        assertEquals(OSType.LINUX.name(), vmAspect.getOs());
        assertEquals(IP_ADDRESSES, vmAspect.getIp());
        assertEquals(4, vmAspect.getNumVCPUs().intValue());
    }

}