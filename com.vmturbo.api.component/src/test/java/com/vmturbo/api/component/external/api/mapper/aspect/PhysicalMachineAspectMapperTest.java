package com.vmturbo.api.component.external.api.mapper.aspect;

import static com.vmturbo.api.component.external.api.mapper.aspect.DiskArrayAspectMapperTest.TEST_EXTERNAL_NAME;
import static com.vmturbo.api.component.external.api.mapper.aspect.DiskArrayAspectMapperTest.TEST_STORAGE_TYPE;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class PhysicalMachineAspectMapperTest extends BaseAspectMapperTest {

    public static final long CONNECTED_ENTITY_ID = 456L;
    private static final String CONNECTED_ENTITY_NAME = "CONNECTED_ENTITY";
    private static final List<String> CONNECTED_ENTITY_NAME_LIST =
        Collections.singletonList(CONNECTED_ENTITY_NAME);
    SearchServiceBlockingStub searchRpc;

    SearchServiceMole searchServiceSpy = Mockito.spy(new SearchServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(searchServiceSpy);

    @Before
    public void init() throws Exception{
        searchRpc = SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }
    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
            .setStorage(StorageInfo.newBuilder()
                .setStorageType(TEST_STORAGE_TYPE)
                .addExternalName(TEST_EXTERNAL_NAME))
            .build();
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
            EntityType.PHYSICAL_MACHINE,
            typeSpecificInfo);
        topologyEntityDTO.addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityId(CONNECTED_ENTITY_ID)
            .setConnectedEntityType(EntityType.PROCESSOR_POOL_VALUE)
            .build());
        when(searchServiceSpy.searchEntities(anyObject())).thenReturn(SearchEntitiesResponse.newBuilder()
            .addEntities(Entity.newBuilder()
                .setOid(CONNECTED_ENTITY_ID)
                .setDisplayName(CONNECTED_ENTITY_NAME)
                .setType(EntityType.PROCESSOR_POOL_VALUE))
            .build());

        PhysicalMachineAspectMapper testMapper = new PhysicalMachineAspectMapper(searchRpc);
        // act
        final EntityAspect resultAspect = testMapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertTrue(resultAspect instanceof PMEntityAspectApiDTO);
        final PMEntityAspectApiDTO pmAspect = (PMEntityAspectApiDTO) resultAspect;
        assertNotNull(pmAspect.getProcessorPools());
        assertEquals(CONNECTED_ENTITY_NAME_LIST, pmAspect.getProcessorPools());
    }

}