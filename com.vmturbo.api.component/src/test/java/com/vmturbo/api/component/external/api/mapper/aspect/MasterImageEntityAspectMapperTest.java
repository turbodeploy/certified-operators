package com.vmturbo.api.component.external.api.mapper.aspect;

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
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.entityaspect.MasterImageEntityAspectApiDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit test for {@link MasterImageEntityAspectMapper}.
 */
public class MasterImageEntityAspectMapperTest extends BaseAspectMapperTest {
    private static final long DESKTOP_POOL_OID = 10001;
    private static final long VM_REFERENCE_ID = 10002;
    private static final long MASTER_IMAGE_OID = 10003;
    private static final String MASTER_IMAGE_VM_DISPLAY_NAME = "S01-P01-P01-00";
    private static final int MASTER_IMAGE_VM_NUM_CPU = 4;
    private static final float MASTER_IMAGE_VMEM_CAPACITY = 500F;
    private static final float MASTER_IMAGE_STORAGE_USED_1 = 100F;
    private static final float MASTER_IMAGE_STORAGE_USED_2 = 200F;
    private static final float DELTA = 0.001F;
    private static final long TEMPLATE_ID = 34985L;

    private TemplateServiceMole templateMole = Mockito.spy(new TemplateServiceMole());
    private MasterImageEntityAspectMapper masterImageEntityAspectMapper;

    private RepositoryApi repositoryApi;
    private TemplateServiceBlockingStub templateService;
    private TopologyEntityDTO masterImageVirtualMachine;
    private TopologyEntityDTO desktopPool;

    /**
     * Rule to define test GRPC server.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(templateMole);

    /**
     * Objects initialization necessary for a unit test.
     */
    @Before
    public void setUp() {
        repositoryApi = Mockito.mock(RepositoryApi.class);
        templateService = TemplateServiceGrpc.newBlockingStub(testServer.getChannel());
        masterImageEntityAspectMapper = new MasterImageEntityAspectMapper(repositoryApi, templateService);

        final List<CommoditySoldDTO> commoditySoldList = ImmutableList.of(
                CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.VMEM_VALUE))
                        .setCapacity(MASTER_IMAGE_VMEM_CAPACITY)
                        .build());
        final List<CommodityBoughtDTO> commodityBoughtDTOList = ImmutableList.of(
                CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.STORAGE_PROVISIONED_VALUE))
                        .setUsed(MASTER_IMAGE_STORAGE_USED_1)
                        .build(), CommodityBoughtDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.STORAGE_PROVISIONED_VALUE))
                        .setUsed(MASTER_IMAGE_STORAGE_USED_2)
                        .build());
        final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders = ImmutableList.of(
                CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(DESKTOP_POOL_OID)
                        .addAllCommodityBought(commodityBoughtDTOList)
                        .build());
        masterImageVirtualMachine = TopologyEntityDTO.newBuilder()
                .setOid(MASTER_IMAGE_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName(MASTER_IMAGE_VM_DISPLAY_NAME)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setNumCpus(MASTER_IMAGE_VM_NUM_CPU)
                                .build())
                        .build())
                .addAllCommoditySoldList(commoditySoldList)
                .addAllCommoditiesBoughtFromProviders(commoditiesBoughtFromProviders)
                .build();

        desktopPool = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.DESKTOP_POOL_VALUE)
                .setOid(DESKTOP_POOL_OID)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setDesktopPool(DesktopPoolInfo.newBuilder()
                                .setVmWithSnapshot(VmWithSnapshot.newBuilder()
                                        .setVmReferenceId(VM_REFERENCE_ID)
                                        .build())
                                .build()))
                .build();
    }

    /**
     * Test map entity to aspect. Сase for desktop pool.
     */
    @Test
    public void testMapEntityToAspectDesktopPool() {
        final SingleEntityRequest masterImageVirtualMachineRequest =
                ApiTestUtils.mockSingleEntityRequest(masterImageVirtualMachine);
        Mockito.when(repositoryApi.entityRequest(VM_REFERENCE_ID))
                .thenReturn(masterImageVirtualMachineRequest);
        // act
        final MasterImageEntityAspectApiDTO aspect =
                masterImageEntityAspectMapper.mapEntityToAspect(desktopPool);
        // assert
        checkAspect(aspect);
    }

    /**
     * Test map entity to aspect. Сase for virtual machine.
     */
    @Test
    public void testMapEntityToAspectVirtualMachineFromDesktopPool() {
        final SingleEntityRequest value =
                ApiTestUtils.mockSingleEntityRequest(masterImageVirtualMachine);
        Mockito.when(repositoryApi.entityRequest(VM_REFERENCE_ID)).thenReturn(value);
        final SingleEntityRequest desktopPoolRequest =
                ApiTestUtils.mockSingleEntityRequest(desktopPool);
        Mockito.when(repositoryApi.entityRequest(DESKTOP_POOL_OID)).thenReturn(desktopPoolRequest);

        final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders = ImmutableList.of(
                CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.DESKTOP_POOL_VALUE)
                        .setProviderId(DESKTOP_POOL_OID)
                        .addAllCommodityBought(Collections.emptyList())
                        .build());
        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setOid(TEST_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName(TEST_DISPLAY_NAME)
                .addAllCommoditiesBoughtFromProviders(commoditiesBoughtFromProviders)
                .build();

        // act
        final MasterImageEntityAspectApiDTO aspect =
                masterImageEntityAspectMapper.mapEntityToAspect(vm);
        // assert
        checkAspect(aspect);
    }

    /**
     * Test the mapping to aspect from a template.
     */
    @Test
    public void testMapEntityToAspectFromTemplate() {
        desktopPool = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.DESKTOP_POOL_VALUE)
                        .setOid(DESKTOP_POOL_OID)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setDesktopPool(DesktopPoolInfo.newBuilder()
                                        .setTemplateReferenceId(TEMPLATE_ID)
                                        .build()))
                        .build();
        float cpu1 = 2F;
        float cpu2 = 3F;
        float storage = MASTER_IMAGE_STORAGE_USED_1;
        TemplateInfo info = TemplateInfo.newBuilder().setTemplateSpecId(1L).setName("qqq")
                        .addResources(TemplateResource.newBuilder()
                                        .setCategory(ResourcesCategory.newBuilder()
                                                        .setName(ResourcesCategoryName.Compute))
                                        .addFields(TemplateField.newBuilder()
                                                        .setName(TemplatesUtils.NUM_OF_CPU)
                                                        .setValue(String.valueOf(cpu1)))
                                        .addFields(TemplateField.newBuilder()
                                                   .setName(TemplatesUtils.MEMORY_SIZE)
                                                   .setValue(String.valueOf(MASTER_IMAGE_VMEM_CAPACITY)))
                                        .build())
                        .addResources(TemplateResource.newBuilder()
                                      .setCategory(ResourcesCategory.newBuilder()
                                                      .setName(ResourcesCategoryName.Compute))
                                      .addFields(TemplateField.newBuilder()
                                                      .setName(TemplatesUtils.NUM_OF_CPU)
                                                      .setValue(String.valueOf(cpu2)))
                                      .build())
                        .addResources(TemplateResource.newBuilder()
                                      .setCategory(ResourcesCategory.newBuilder()
                                                      .setName(ResourcesCategoryName.Storage))
                                      .addFields(TemplateField.newBuilder()
                                                      .setName(TemplatesUtils.DISK_SIZE)
                                                      .setValue(String.valueOf(storage)))
                                      .build())
                        .build();
        Mockito.when(templateMole.getTemplate(Mockito.any())).thenReturn(SingleTemplateResponse
                        .newBuilder()
                        .setTemplate(Template.newBuilder().setId(1L).setTemplateInfo(info).build()).build());

        final MasterImageEntityAspectApiDTO aspect =
                masterImageEntityAspectMapper.mapEntityToAspect(desktopPool);
        Assert.assertEquals(MASTER_IMAGE_VMEM_CAPACITY, aspect.getMem(), DELTA);
        Assert.assertEquals(cpu1 + cpu2, aspect.getNumVcpus(), DELTA);
        Assert.assertEquals(MASTER_IMAGE_STORAGE_USED_1, aspect.getStorage(), DELTA);
    }

    private void checkAspect(MasterImageEntityAspectApiDTO aspect) {
        Assert.assertEquals(MASTER_IMAGE_VM_DISPLAY_NAME, aspect.getDisplayName());
        Assert.assertEquals(MASTER_IMAGE_VMEM_CAPACITY, aspect.getMem(), DELTA);
        Assert.assertEquals(MASTER_IMAGE_VM_NUM_CPU, (int)aspect.getNumVcpus());
        Assert.assertEquals(MASTER_IMAGE_STORAGE_USED_1 +
                MASTER_IMAGE_STORAGE_USED_2, aspect.getStorage(), DELTA);
    }
}
