package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

public class VirtualMachineEntityConstructorTest {
    private double epsilon = 1e-5;

    private final static Template VM_TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(TemplateConverterTestUtil.VM_TEMPLATE_INFO)
        .build();

    private final Map<Long, TopologyEntity.Builder> topology = Collections.emptyMap();

    @Test
    public void testVMConvert() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);

        final TopologyEntityDTO.Builder topologyEntityDTO = new VirtualMachineEntityConstructor()
                .createTopologyEntityFromTemplate(VM_TEMPLATE, topology, null, false,
                        identityProvider, null);

        assertEquals(3, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(2, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(400.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VCPU_VALUE), epsilon);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VMEM_VALUE), epsilon);
        assertEquals(300.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VSTORAGE_VALUE), epsilon);
        assertEquals(10.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.MEM_VALUE), epsilon);
        assertEquals(100.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.MEM_PROVISIONED_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.STORAGE_ACCESS_VALUE), epsilon);
        assertEquals(30.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.STORAGE_AMOUNT_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_PROVISIONED_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.IO_THROUGHPUT_VALUE), epsilon);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.NET_THROUGHPUT_VALUE), epsilon);
        assertEquals(40.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_VALUE), epsilon);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.CPU_PROVISIONED_VALUE), epsilon);
    }

    @Test
    public void testVMConvertWithConstraints() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);

        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder().setOid(1)
                .setEntityType(10)
                .addAllCommoditySoldList(TemplateConverterTestUtil.VM_COMMODITY_SOLD)
                .addAllCommoditiesBoughtFromProviders(
                        TemplateConverterTestUtil.VM_COMMODITY_BOUGHT_FROM_PROVIDER);

        final TopologyEntityDTO.Builder topologyEntityDTO = new VirtualMachineEntityConstructor()
                .createTopologyEntityFromTemplate(VM_TEMPLATE, topology, builder, false,
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

    @Test
    public void testReservationVMConvert() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);

        TopologyEntityDTO.Builder topologyEntityDTO = new VirtualMachineEntityConstructor(true)
                .createTopologyEntityFromTemplate(VM_TEMPLATE, topology, null, false,
                        identityProvider, null);
        assertEquals(3, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(2, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(400.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
                CommodityType.VCPU_VALUE), epsilon);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
                CommodityType.VMEM_VALUE), epsilon);
        assertEquals(300.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
                CommodityType.VSTORAGE_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.MEM_VALUE), epsilon);
        assertEquals(100.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.MEM_PROVISIONED_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_ACCESS_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_AMOUNT_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_PROVISIONED_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.IO_THROUGHPUT_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.NET_THROUGHPUT_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.CPU_VALUE), epsilon);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.CPU_PROVISIONED_VALUE), epsilon);
    }
}
