package com.vmturbo.topology.graph;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.function.BiFunction;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.topology.graph.SearchableProps.VmProps;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;

/**
 * Unit tests for {@link ThinSearchableProps}.
 */
public class ThinSearchablePropsTest {

    /**
     * Checks isHotAddSupported method result if the commodityType is correct or invalid.
     */
    @Test
    public void testHotAddSupported() {
        final TagIndex tags = DefaultTagIndex.singleEntity(0L, Tags.newBuilder().build());
        final VmProps vm = getVmProps(tags, Builder::setHotAddSupported);
        testVmHotChangeSupported(vm, VmProps::isHotAddSupported);
    }

    /**
     * Checks isHotRemoveSupported method result if the commodityType is correct or invalid.
     */
    @Test
    public void testHotRemoveSupported() {
        final TagIndex tags = DefaultTagIndex.singleEntity(0L, Tags.newBuilder().build());
        final VmProps vm = getVmProps(tags, Builder::setHotRemoveSupported);
        testVmHotChangeSupported(vm, VmProps::isHotRemoveSupported);
    }

    /**
     * Checks DB server parameters correctness.
     */
    @Test
    public void testDBServer() {
        final TagIndex tags = DefaultTagIndex.singleEntity(0L, Tags.newBuilder().build());
        TopologyDTO.TypeSpecificInfo.DatabaseInfo dbInfo = TopologyDTO.TypeSpecificInfo.DatabaseInfo.newBuilder()
                .setEngine(CloudCostDTO.DatabaseEngine.MARIADB)
                .setEdition(CloudCostDTO.DatabaseEdition.EXPRESS)
                .setVersion("1.0.1")
                .build();
        final SearchableProps.DatabaseServerProps dbProps = (SearchableProps.DatabaseServerProps)ThinSearchableProps
                .newProps(tags,
                    TestGraphEntity.newBuilder(0L, ApiEntityType.DATABASE_SERVER)
                        .setTypeSpecificInfo(TopologyDTO.TypeSpecificInfo
                                .newBuilder()
                                .setDatabase(dbInfo)
                                .build())
                            .build(),
                    TopologyEntityDTO.newBuilder()
                            .setOid(0L)
                            .setDisplayName("Test_DisplayName")
                            .setEntityType(EntityType.DATABASE_SERVER_VALUE)
                            .setTypeSpecificInfo(TopologyDTO.TypeSpecificInfo
                                    .newBuilder()
                                    .setDatabase(dbInfo)
                                    .build())
                            .build()
                );
        assertThat(dbProps.getDatabaseEngine(), is(dbInfo.getEngine()));
        assertThat(dbProps.getDatabaseEdition(), is(dbInfo.getEdition()));
        assertThat(dbProps.getDatabaseVersion(), is(dbInfo.getVersion()));
    }

    private static void testVmHotChangeSupported(VmProps vm,
            BiFunction<VmProps, Integer, Boolean> function) {
        Assert.assertEquals(false, function.apply(vm, CommodityType.VMEM.getNumber()));
        Assert.assertEquals(true, function.apply(vm, CommodityType.VCPU.getNumber()));
    }

    private static VmProps getVmProps(TagIndex tags,
            BiFunction<Builder, Boolean, Builder> function) {
        return (VmProps)ThinSearchableProps.newProps(tags,
                TestGraphEntity.newBuilder(0L, ApiEntityType.VIRTUAL_MACHINE)
                        .addCommSold(CommoditySoldDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                        .setType(CommodityType.VCPU.getNumber())
                                        .build())
                                .setHotResizeInfo(
                                        function.apply(HotResizeInfo.newBuilder(), true).build())
                                .build())
                        .build(), TopologyEntityDTO.newBuilder()
                        .setOid(0L)
                        .setDisplayName("Test_DisplayName")
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build());
    }
}
