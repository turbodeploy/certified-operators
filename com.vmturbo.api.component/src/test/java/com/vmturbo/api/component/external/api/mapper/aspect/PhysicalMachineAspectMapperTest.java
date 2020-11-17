package com.vmturbo.api.component.external.api.mapper.aspect;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.dto.entityaspect.PMDiskAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.PMDiskGroupAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.PMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class PhysicalMachineAspectMapperTest extends BaseAspectMapperTest {

    private static final long CONNECTED_ENTITY_ID = 456L;
    private static final String CONNECTED_ENTITY_NAME = "CONNECTED_ENTITY";
    private static final List<String> CONNECTED_ENTITY_NAME_LIST =
        Collections.singletonList(CONNECTED_ENTITY_NAME);

    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private static final String EXAMPLE_CPU_MODEL = "Quad-Core AMD Opteron(tm) Processor 8386 SE";
    private static final String NETWORK_COMMODITY_NAME = "Network1";

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
        final PMEntityAspectApiDTO pmAspect = testMapper.mapEntityToAspect(topologyEntityDTO.build());

        verify(repositoryApi).entitiesRequest(Collections.singleton(CONNECTED_ENTITY_ID));
        verify(mockReq).getMinimalEntities();

        // assert
        assertNotNull(pmAspect.getProcessorPools());
        assertEquals(CONNECTED_ENTITY_NAME_LIST, pmAspect.getProcessorPools());
    }

    /**
     * Test vSAN host for disk groups.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testDiskGroups() throws Exception {
        InputStream is = PhysicalMachineAspectMapperTest.class
                .getResourceAsStream("/PhysicalMachineAspectMapperTest.json");
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(is), builder);

        PhysicalMachineAspectMapper mapper = new PhysicalMachineAspectMapper(null);
        final PMEntityAspectApiDTO result = mapper.mapEntityToAspect(builder.build());

        List<PMDiskGroupAspectApiDTO> diskGroups = result.getDiskGroups();
        Assert.assertTrue(diskGroups.size() > 0);

        for (PMDiskGroupAspectApiDTO group : diskGroups) {
            List<PMDiskAspectApiDTO> disks = group.getDisks();
            Assert.assertTrue(disks.size() > 0);

            for (PMDiskAspectApiDTO disk : disks) {
                Assert.assertTrue(disk.getDiskCapacity() > 0);
                Assert.assertNotNull(disk.getDiskRole());
            }
        }
    }

    /**
     * A physical machine without a cpu model should not cause any exceptions.
     */
    @Test
    public void testWithoutCpuModel() {
        // no type specific info
        TopologyEntityDTO noCpuModelEntity = TopologyEntityDTO.newBuilder()
            .buildPartial();
        checkCpuModel(noCpuModelEntity, null);

        // no physicalMachineInfo
        noCpuModelEntity = TopologyEntityDTO.newBuilder()
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .buildPartial())
            .buildPartial();
        checkCpuModel(noCpuModelEntity, null);

        // no cpuModel
        noCpuModelEntity = TopologyEntityDTO.newBuilder()
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(PhysicalMachineInfo.newBuilder()
                    .buildPartial())
                .buildPartial())
            .buildPartial();
        checkCpuModel(noCpuModelEntity, null);
    }

    /**
     * A physical machine with a cpu model should be extracted and placed in the result.
     */
    @Test
    public void testWithCpuModel() {
        TopologyEntityDTO cpuModelEntity = TopologyEntityDTO.newBuilder()
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(PhysicalMachineInfo.newBuilder()
                    .setCpuModel(EXAMPLE_CPU_MODEL)
                    .buildPartial())
                .buildPartial())
            .buildPartial();
        checkCpuModel(cpuModelEntity, EXAMPLE_CPU_MODEL);
    }

    private void checkCpuModel(TopologyEntityDTO entityDto, String expectedCpuModel) {
        PhysicalMachineAspectMapper mapper = new PhysicalMachineAspectMapper(repositoryApi);
        final PMEntityAspectApiDTO pmEntityAspect = mapper.mapEntityToAspect(entityDto);
        Assert.assertNotNull(pmEntityAspect);
        Assert.assertEquals(expectedCpuModel, pmEntityAspect.getCpuModel());
    }

    /**
     * Test network mapping with no network commodities sold.
     */
    @Test
    public void testNetworksNoCommodities() {
        final PhysicalMachineAspectMapper mapper = new PhysicalMachineAspectMapper(repositoryApi);
        final TopologyEntityDTO emptyEntityDto = TopologyEntityDTO.newBuilder()
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(PhysicalMachineInfo.newBuilder()
                    .buildPartial())
                .buildPartial())
            .buildPartial();
        final PMEntityAspectApiDTO noNetworksEntityAspect = mapper.mapEntityToAspect(emptyEntityDto);
        Assert.assertNotNull(noNetworksEntityAspect.getConnectedNetworks());
        Assert.assertEquals(0, noNetworksEntityAspect.getConnectedNetworks().size());
    }


    /**
     * Test network mapping.
     */
    @Test
    public void testNetworks() {
        final PhysicalMachineAspectMapper mapper = new PhysicalMachineAspectMapper(repositoryApi);

        final CommoditySoldDTO networkCommNoName = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.NETWORK_VALUE).build())
            .build();
        final CommoditySoldDTO networkComm = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.NETWORK_VALUE).build())
            .setDisplayName(NETWORK_COMMODITY_NAME)
            .build();
        final TopologyEntityDTO entityDto = TopologyEntityDTO.newBuilder()
            .addAllCommoditySoldList(ImmutableList.of(networkCommNoName, networkComm))
            .buildPartial();
        final PMEntityAspectApiDTO networkEntityAspect = mapper.mapEntityToAspect(entityDto);
        Assert.assertEquals(1, networkEntityAspect.getConnectedNetworks().size());
        Assert.assertEquals(NETWORK_COMMODITY_NAME, networkEntityAspect.getConnectedNetworks().get(0));
    }
}