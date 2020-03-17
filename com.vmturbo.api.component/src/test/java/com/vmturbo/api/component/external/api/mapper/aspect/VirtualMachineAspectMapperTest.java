package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VMEntityAspectApiDTO;
import com.vmturbo.api.dto.user.BusinessUserSessionApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessUserInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

public class VirtualMachineAspectMapperTest extends BaseAspectMapperTest {

    private static final long BUSINESS_USER_OID = 1000L;
    private static final long VIRTUAL_MACHINE_OTHER_OID = 1001L;
    private static final long SESSION_DURATION_TO_THIS_VM = 10000000L;
    private static final long SESSION_DURATION_TO_OTHER_VM = 20000000L;

    private static final String CONNECTED_NETWORK_NAME_1 = "network 1";
    private static final String CONNECTED_NETWORK_NAME_2 = "network 2";
    private static final List<String> CONNECTED_ENTITY_NAME_LIST = ImmutableList.of(
            CONNECTED_NETWORK_NAME_1, CONNECTED_NETWORK_NAME_2);
    private static final List<String> IP_ADDRESSES = ImmutableList.of("1.2.3.4", "5.6.7.8");

    private final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
    private TopologyEntityDTO businessUser;

    /**
     * Objects initialization necessary for a unit test.
     */
    @Before
    public void setUp() {
        businessUser = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_USER_VALUE)
                .setOid(BUSINESS_USER_OID)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setBusinessUser(BusinessUserInfo.newBuilder()
                                .putAllVmOidToSessionDuration(
                                        ImmutableMap.of(TEST_OID, SESSION_DURATION_TO_THIS_VM,
                                                VIRTUAL_MACHINE_OTHER_OID,
                                                SESSION_DURATION_TO_OTHER_VM))
                                .build())
                        .build())
                .build();
        final SearchRequest request =
                ApiTestUtils.mockSearchFullReq(Collections.singletonList(businessUser));
        Mockito.when(repositoryApi.newSearchRequest(Mockito.any())).thenReturn(request);
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
                .addConnectedNetworks(CONNECTED_NETWORK_NAME_1)
                .addConnectedNetworks(CONNECTED_NETWORK_NAME_2)
            )
            .build();
        final TopologyEntityDTO topologyEntityDTO =
                topologyEntityDTOBuilder(EntityType.VIRTUAL_MACHINE, typeSpecificInfo)
                        .putEntityPropertyMap(StringConstants.EBS_OPTIMIZED, Boolean.TRUE.toString())
                        .build();

        VirtualMachineAspectMapper testMapper = new VirtualMachineAspectMapper(repositoryApi);

        // act
        final EntityAspect resultAspect = testMapper.mapEntityToAspect(topologyEntityDTO);

        // assert
        final VMEntityAspectApiDTO vmAspect = (VMEntityAspectApiDTO)resultAspect;
        Assert.assertEquals(OSType.LINUX.name(), vmAspect.getOs());
        Assert.assertEquals(IP_ADDRESSES, vmAspect.getIp());
        Assert.assertEquals(4, vmAspect.getNumVCPUs().intValue());
        Assert.assertEquals(CONNECTED_ENTITY_NAME_LIST, vmAspect.getConnectedNetworks()
                .stream()
                .map(BaseApiDTO::getDisplayName)
                .collect(Collectors.toList()));

        Assert.assertEquals(1, vmAspect.getSessions().size());
        final BusinessUserSessionApiDTO sessionApiDTO = vmAspect.getSessions().get(0);
        Assert.assertEquals(BUSINESS_USER_OID,
                (long)Long.valueOf(sessionApiDTO.getBusinessUserUuid()));
        Assert.assertEquals(TEST_OID, (long)Long.valueOf(sessionApiDTO.getConnectedEntityUuid()));
        Assert.assertEquals(SESSION_DURATION_TO_THIS_VM, (long)sessionApiDTO.getDuration());
        Assert.assertTrue(vmAspect.isEbsOptimized());
        Assert.assertNull(vmAspect.isAHUBLicense());
    }

    /**
     * Test that we set the isAHUB aspect to TRUE is case that this is Azure Hybrid Benefit (AHuB).
     */
    @Test
    public void testMapEntityToAspectIsAhub() {
        final TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                        .setGuestOsInfo(OS.newBuilder()
                                .setGuestOsType(OSType.WINDOWS)
                                .setGuestOsName(OSType.WINDOWS.name()))
                        .setNumCpus(4)
                        .setLicenseModel(LicenseModel.AHUB)
                        .addIpAddresses(IpAddress.newBuilder().setIpAddress(IP_ADDRESSES.get(0)))
                        .addConnectedNetworks(CONNECTED_NETWORK_NAME_1)
                )
                .build();
        final TopologyEntityDTO topologyEntityDTO =
                topologyEntityDTOBuilder(EntityType.VIRTUAL_MACHINE,
                        typeSpecificInfo).putEntityPropertyMap(StringConstants.EBS_OPTIMIZED,
                        Boolean.TRUE.toString()).build();

        VirtualMachineAspectMapper testMapper = new VirtualMachineAspectMapper(repositoryApi);
        // act
        final EntityAspect resultAspect = testMapper.mapEntityToAspect(topologyEntityDTO);
        // assert
        final VMEntityAspectApiDTO vmAspect = (VMEntityAspectApiDTO)resultAspect;
        Assert.assertTrue(vmAspect.isAHUBLicense());
    }
}
