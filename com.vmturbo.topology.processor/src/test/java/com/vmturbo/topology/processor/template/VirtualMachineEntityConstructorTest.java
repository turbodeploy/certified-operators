package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBought;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySold;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelScaleFactorResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityMoles.CpuCapacityServiceMole;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TopologyEntityConstructor.TemplateActionType;

/**
 * Verifies the conversion of VM Templates to TopologyEntityDTOs.
 */
public class VirtualMachineEntityConstructorTest {

    private static final double EPSILON = 1e-5;

    private static final Template VM_TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(TemplateConverterTestUtil.VM_TEMPLATE_INFO)
        .build();

    private static final String CPU_MODEL = "Intel Xeon Gold 5115";
    private static final double NO_SCALING = 1.0;
    private static final double CPU_MODEL_SCALING = 3.0;

    private static final Template VM_CPU_MODEL_TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(TemplateConverterTestUtil.VM_TEMPLATE_INFO.toBuilder()
            .setCpuModel(CPU_MODEL))
        .build();

    private final Map<Long, TopologyEntity.Builder> topology = Collections.emptyMap();

    private final CpuCapacityServiceMole cpuCapacityServiceMole =  spy(new CpuCapacityServiceMole());

    /**
     * Must be public, otherwise when unit test starts up, JUnit throws ValidationError.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(cpuCapacityServiceMole);

    /**
     * Cannot be initialized in the field, otherwise a IllegalStateException: GrpcTestServer has not
     * been started yet. Please call start() before is thrown.
     */
    private CpuCapacityServiceBlockingStub cpuCapacityService;

    /**
     * Setup the mocked services that cannot be initialized as fields.
     */
    @Before
    public void setup() {
        cpuCapacityService = CpuCapacityServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * All the vm template's settings should also be converted to commodities on a
     * TopologyEntityDTO.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testVMConvert() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);

        final TopologyEntityDTO.Builder topologyEntityDTO = new VirtualMachineEntityConstructor(
            cpuCapacityService)
                .createTopologyEntityFromTemplate(VM_TEMPLATE, topology, null, TemplateActionType.CLONE,
                        identityProvider, null);

        assertEquals(3, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(2, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(400.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VCPU_VALUE), EPSILON);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VMEM_VALUE), EPSILON);
        assertEquals(300.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VSTORAGE_VALUE), EPSILON);
        assertEquals(10.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.MEM_VALUE), EPSILON);
        assertEquals(100.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.MEM_PROVISIONED_VALUE), EPSILON);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.STORAGE_ACCESS_VALUE), EPSILON);
        assertEquals(30.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.STORAGE_AMOUNT_VALUE), EPSILON);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_PROVISIONED_VALUE), EPSILON);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.IO_THROUGHPUT_VALUE), EPSILON);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.NET_THROUGHPUT_VALUE), EPSILON);
        assertEquals(40.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_VALUE), EPSILON);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.CPU_PROVISIONED_VALUE), EPSILON);
    }

    /**
     * No cpu model on the template should set CPU scaling factors to default 1.0.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testNoCpuModel() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);
        when(cpuCapacityServiceMole.getCpuScaleFactors(any())).thenReturn(
            CpuModelScaleFactorResponse.newBuilder()
                .putScaleFactorByCpuModel(CPU_MODEL, CPU_MODEL_SCALING)
                .build());

        final TopologyEntityDTO.Builder topologyEntityDTO = new VirtualMachineEntityConstructor(
            cpuCapacityService)
            .createTopologyEntityFromTemplate(VM_TEMPLATE, topology, null, TemplateActionType.CLONE,
                identityProvider, null);

        assertEquals(NO_SCALING, getCommoditySold(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VCPU_VALUE).get().getScalingFactor(), EPSILON);
        assertEquals(NO_SCALING, getCommodityBought(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_VALUE).get().getScalingFactor(), EPSILON);
        assertEquals(NO_SCALING, getCommodityBought(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_PROVISIONED_VALUE).get().getScalingFactor(), EPSILON);
    }

    /**
     * No cpu model found in the service should set CPU scaling factors to default 1.0.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testCpuModelButNotFoundInService() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);
        when(cpuCapacityServiceMole.getCpuScaleFactors(any())).thenReturn(
            CpuModelScaleFactorResponse.newBuilder().build());

        final TopologyEntityDTO.Builder topologyEntityDTO = new VirtualMachineEntityConstructor(
            cpuCapacityService)
            .createTopologyEntityFromTemplate(VM_CPU_MODEL_TEMPLATE, topology, null, TemplateActionType.CLONE,
                identityProvider, null);

        assertEquals(NO_SCALING, getCommoditySold(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VCPU_VALUE).get().getScalingFactor(), EPSILON);
        assertEquals(NO_SCALING, getCommodityBought(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_VALUE).get().getScalingFactor(), EPSILON);
        assertEquals(NO_SCALING, getCommodityBought(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_PROVISIONED_VALUE).get().getScalingFactor(), EPSILON);
    }

    /**
     * Cpu model on the template should set CPU scaling factors to the 3.0 that the
     * CpuCapacityService returns.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testCpuModel() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);
        when(cpuCapacityServiceMole.getCpuScaleFactors(any())).thenReturn(
            CpuModelScaleFactorResponse.newBuilder()
                .putScaleFactorByCpuModel(CPU_MODEL, CPU_MODEL_SCALING)
                .build());

        final TopologyEntityDTO.Builder topologyEntityDTO = new VirtualMachineEntityConstructor(
            cpuCapacityService)
            .createTopologyEntityFromTemplate(VM_CPU_MODEL_TEMPLATE, topology, null, TemplateActionType.CLONE,
                identityProvider, null);

        assertEquals(CPU_MODEL_SCALING, getCommoditySold(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VCPU_VALUE).get().getScalingFactor(), EPSILON);
        assertEquals(CPU_MODEL_SCALING, getCommodityBought(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_VALUE).get().getScalingFactor(), EPSILON);
        assertEquals(CPU_MODEL_SCALING, getCommodityBought(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_PROVISIONED_VALUE).get().getScalingFactor(), EPSILON);
    }

    /**
     * The constraint commodities on a vm template should also be copied to the TopologyEntityDTO.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testVMConvertWithConstraints() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);

        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder().setOid(1)
                .setEntityType(10)
                .addAllCommoditySoldList(TemplateConverterTestUtil.VM_COMMODITY_SOLD)
                .addAllCommoditiesBoughtFromProviders(
                        TemplateConverterTestUtil.VM_COMMODITY_BOUGHT_FROM_PROVIDER);

        final TopologyEntityDTO.Builder topologyEntityDTO = new VirtualMachineEntityConstructor(
            cpuCapacityService)
                .createTopologyEntityFromTemplate(VM_TEMPLATE, topology, builder, TemplateActionType.CLONE,
                        identityProvider, null);

        assertEquals(4, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(1,
                topologyEntityDTO.getCommoditySoldListList().stream()
                        .filter(commoditySoldDTO -> commoditySoldDTO.getCommodityType()
                                .getType() == CommodityType.APPLICATION_VALUE)
                        .count());
        assertEquals(2, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals("123-network",
                TemplateConverterTestUtil.getCommodityBoughtKey(
                        topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                        CommodityType.NETWORK_VALUE));
        assertEquals("123-datastore",
                TemplateConverterTestUtil.getCommodityBoughtKey(
                        topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                        CommodityType.DATASTORE_VALUE));
        assertEquals("123-data-center",
                TemplateConverterTestUtil.getCommodityBoughtKey(
                        topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                        CommodityType.DATACENTER_VALUE));
        assertEquals("123-cluster",
                TemplateConverterTestUtil.getCommodityBoughtKey(
                        topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                        CommodityType.CLUSTER_VALUE));
        assertEquals("123-storage-cluster",
                TemplateConverterTestUtil.getCommodityBoughtKey(
                        topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                        CommodityType.STORAGE_CLUSTER_VALUE));
        assertEquals("123-dspm-access",
                TemplateConverterTestUtil.getCommodityBoughtKey(
                        topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                        CommodityType.DSPM_ACCESS_VALUE));
        assertEquals("123-extent",
                TemplateConverterTestUtil.getCommodityBoughtKey(
                        topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                        CommodityType.EXTENT_VALUE));
    }
}
