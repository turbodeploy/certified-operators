package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.mockito.Mockito.spy;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.RegionAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.GeoDataInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.RegionInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


/**
 * RegionAspectMapperTest test class for Region aspect mapper.
 */
public class RegionAspectMapperTest extends BaseAspectMapperTest {

    private static final long REGION_OID = 1001L;
    private static final long REGION_OID_2 = 1002L;
    private static final long REGION_OID_3 = 1003L;
    private TopologyEntityDTO regionTopologyEntityDTO1;
    private TopologyEntityDTO regionTopologyEntityDTO2;
    private TopologyEntityDTO regionTopologyEntityDTO3;

    /**
     * Mock server.
     */
    @Rule
    public GrpcTestServer mockServer = GrpcTestServer.newServer(spy(new SupplyChainServiceMole()));

    private SupplyChainServiceBlockingStub supplyChainServiceClient;

    /**
     * Set up test and create a topology entity dto.
     **/
    @Before
    public void setUp() {
        regionTopologyEntityDTO1 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION_OID)
                .setTypeSpecificInfo(
                        TypeSpecificInfo.newBuilder()
                                .setRegion(RegionInfo.newBuilder()
                                        .setGeoData(GeoDataInfo.newBuilder()
                                                .setLatitude(10.0)
                                                .setLongitude(10.0))
                                )
                )
                .build();

        regionTopologyEntityDTO2 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION_OID_2)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder())
                .build();

        regionTopologyEntityDTO3 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION_OID_3)
                .setTypeSpecificInfo(
                        TypeSpecificInfo.newBuilder()
                                .setRegion(RegionInfo.newBuilder())
                )
                .build();

        supplyChainServiceClient = SupplyChainServiceGrpc.newBlockingStub(mockServer.getChannel());
    }

    /**
     * Map entity to its aspect and verify if the information is correct.
     **/
    @Test
    public void testMapEntityToAspect() {
        RegionAspectMapper testMapper = new RegionAspectMapper(supplyChainServiceClient);

        // act
        final Optional<Map<Long, EntityAspect>> resultAspect1 = testMapper.mapEntityToAspectBatch(Collections.singletonList(regionTopologyEntityDTO1));

        // assert
        final RegionAspectApiDTO regionAspectApiDTO1 = (RegionAspectApiDTO)resultAspect1.get().get(REGION_OID);
        assert regionAspectApiDTO1 != null;
        Assert.assertEquals(10.0, regionAspectApiDTO1.getLatitude(), 0);
        Assert.assertEquals(10.0, regionAspectApiDTO1.getLongitude(), 0);
        Assert.assertEquals(0, regionAspectApiDTO1.getNumWorkloads());

        // act
        final EntityAspect resultAspect2 = testMapper.mapEntityToAspect(regionTopologyEntityDTO2);

        // assert
        final RegionAspectApiDTO regionAspectApiDTO2 = (RegionAspectApiDTO)resultAspect2;
        assert regionAspectApiDTO2 == null;

        // act
        final EntityAspect resultAspect3 = testMapper.mapEntityToAspect(regionTopologyEntityDTO3);

        // assert
        final RegionAspectApiDTO regionAspectApiDTO3 = (RegionAspectApiDTO)resultAspect3;
        assert regionAspectApiDTO3 == null;
    }
}
