package com.vmturbo.api.component.external.api.mapper.aspect;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class PhysicalMachineAspectMapperTest extends BaseAspectMapperTest {

    private static final long CONNECTED_ENTITY_ID = 456L;
    private static final String CONNECTED_ENTITY_NAME = "CONNECTED_ENTITY";
    private static final List<String> CONNECTED_ENTITY_NAME_LIST =
        Collections.singletonList(CONNECTED_ENTITY_NAME);

    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);

    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
            .setPhysicalMachine(PhysicalMachineInfo.newBuilder())
            .build();
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
            EntityType.PHYSICAL_MACHINE,
            typeSpecificInfo);
        topologyEntityDTO.addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityId(CONNECTED_ENTITY_ID)
            .setConnectedEntityType(EntityType.PROCESSOR_POOL_VALUE)
            .build());

        final MultiEntityRequest mockReq = ApiTestUtils.mockMultiMinEntityReq(
            Lists.newArrayList(MinimalEntity.newBuilder()
                .setOid(CONNECTED_ENTITY_ID)
                .setDisplayName(CONNECTED_ENTITY_NAME)
                .setEntityType(EntityType.PROCESSOR_POOL_VALUE)
                .build()));
        when(repositoryApi.entitiesRequest(eq(Collections.singleton(CONNECTED_ENTITY_ID))))
            .thenReturn(mockReq);

        PhysicalMachineAspectMapper testMapper = new PhysicalMachineAspectMapper(repositoryApi);
        // act
        final EntityAspect resultAspect = testMapper.mapEntityToAspect(topologyEntityDTO.build());

        verify(repositoryApi).entitiesRequest(Collections.singleton(CONNECTED_ENTITY_ID));
        verify(mockReq).getMinimalEntities();

        // assert
        assertTrue(resultAspect instanceof PMEntityAspectApiDTO);
        final PMEntityAspectApiDTO pmAspect = (PMEntityAspectApiDTO) resultAspect;
        assertNotNull(pmAspect.getProcessorPools());
        assertEquals(CONNECTED_ENTITY_NAME_LIST, pmAspect.getProcessorPools());
    }

}