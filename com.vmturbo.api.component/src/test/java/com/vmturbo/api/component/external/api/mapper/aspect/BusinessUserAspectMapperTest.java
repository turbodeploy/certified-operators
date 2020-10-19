package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.dto.entityaspect.BusinessUserEntityAspectApiDTO;
import com.vmturbo.api.dto.user.BusinessUserSessionApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessUserInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link BusinessUserAspectMapper}.
 */
public class BusinessUserAspectMapperTest {
    private static final String IP_FIRST_VM = "1.2.3.4";
    private static final long OID_FIRST_VM = 222L;
    private static final long OID_SECOND_VM = 333L;
    private static final String IP_SECOND_VM = "5.6.7.8";
    private static final long OID_BUSINESS_USER = 111L;
    private static final long SESSION_DURATION_FIRST_VM = 10000000L;
    private static final long SESSION_DURATION_SECOND_VM = 20000000L;
    private final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    /**
     * Test for {@link BusinessUserAspectMapper#mapEntityToAspect(TopologyEntityDTO)} method.
     */
    @Test
    public void testMapEntityToAspect() {
        final TopologyEntityDTO firstVirtualMachine = TopologyEntityDTO.newBuilder().setEntityType(
                EntityType.VIRTUAL_MACHINE_VALUE).setOid(OID_FIRST_VM).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .addIpAddresses(IpAddress.newBuilder().setIpAddress(IP_FIRST_VM)))
                        .build()).build();
        final TopologyEntityDTO secondVirtualMachine = TopologyEntityDTO.newBuilder().setEntityType(
                EntityType.VIRTUAL_MACHINE_VALUE).setOid(OID_SECOND_VM).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .addIpAddresses(IpAddress.newBuilder().setIpAddress(IP_SECOND_VM)))
                        .build()).build();
        final TopologyEntityDTO businessUser = TopologyEntityDTO.newBuilder().setEntityType(
                EntityType.BUSINESS_USER_VALUE).setOid(OID_BUSINESS_USER).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder()
                        .setBusinessUser(BusinessUserInfo.newBuilder()
                                .putVmOidToSessionDuration(firstVirtualMachine.getOid(),
                                        SESSION_DURATION_FIRST_VM)
                                .putVmOidToSessionDuration(secondVirtualMachine.getOid(),
                                        SESSION_DURATION_SECOND_VM)
                                .build())
                        .build()).build();
        final SearchRequest request = ApiTestUtils.mockSearchFullReq(
                Collections.singletonList(businessUser));
        Mockito.when(repositoryApi.newSearchRequest(Mockito.any())).thenReturn(request);

        final BusinessUserAspectMapper businessUserAspectMapper = new BusinessUserAspectMapper();
        final BusinessUserEntityAspectApiDTO businessUserEntityAspectApiDTO =
                businessUserAspectMapper.mapEntityToAspect(businessUser);
        final List<BusinessUserSessionApiDTO> sessions =
                businessUserEntityAspectApiDTO.getSessions();
        Assert.assertNotNull(sessions);
        Assert.assertEquals(2, sessions.size());
        final Iterator<BusinessUserSessionApiDTO> iterator = sessions.iterator();
        final BusinessUserSessionApiDTO firstBusinessUserSession = iterator.next();
        Assert.assertEquals(businessUser.getOid(),
                Long.parseLong(firstBusinessUserSession.getBusinessUserUuid()));
        Assert.assertEquals(firstVirtualMachine.getOid(),
                Long.parseLong(firstBusinessUserSession.getConnectedEntityUuid()));
        Assert.assertEquals(Long.valueOf(SESSION_DURATION_FIRST_VM),
                firstBusinessUserSession.getDuration());
        final BusinessUserSessionApiDTO secondBusinessUserSession = iterator.next();
        Assert.assertEquals(businessUser.getOid(),
                Long.parseLong(secondBusinessUserSession.getBusinessUserUuid()));
        Assert.assertEquals(secondVirtualMachine.getOid(),
                Long.parseLong(secondBusinessUserSession.getConnectedEntityUuid()));
        Assert.assertEquals(Long.valueOf(SESSION_DURATION_SECOND_VM),
                secondBusinessUserSession.getDuration());
    }
}
