package com.vmturbo.common.protobuf.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * FlyweightTopologyProtoTest.
 */
public class FlyweightTopologyProtoTest {

    private final FlyweightTopologyProto flyweights = new FlyweightTopologyProto();

    private final CommodityBoughtDTO.Builder commodityBought = CommodityBoughtDTO.newBuilder()
        .setCommodityType(CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.BALLOONING_VALUE)
            .setKey("foo"))
        .setUsed(1.48924)
        .setDisplayName("display_name")
        .setScalingFactor(3.0)
        .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(2.0))
        .setHistoricalPeak(HistoricalValues.newBuilder().setPercentile(3.0));

    private final CommoditySoldDTO.Builder commoditySold = CommoditySoldDTO.newBuilder()
        .setCommodityType(CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.BALLOONING_VALUE)
            .setKey("foo"))
        .setUsed(1.80238924)
        .setDisplayName("display_name")
        .setScalingFactor(3.33)
        .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(2.5))
        .setHistoricalPeak(HistoricalValues.newBuilder().setPercentile(3.5))
        .setHotResizeInfo(HotResizeInfo.newBuilder().setHotAddSupported(true).setHotRemoveSupported(false));

    private final AnalysisSettings.Builder analysisSettings = AnalysisSettings.newBuilder()
        .setCloneable(false)
        .setRateOfResize(3.0f)
        .setSuspendable(true);

    private final Tags.Builder tags = Tags.newBuilder()
        .putTags("tag", TagValuesDTO.newBuilder().addValues("my_tag").addValues("other_tag").build());

    private final TypeSpecificInfo.Builder tsInfo = TypeSpecificInfo.newBuilder()
        .setPhysicalMachine(PhysicalMachineInfo.newBuilder()
            .setCpuCoreMhz(1)
            .setCpuModel("my_model")
            .setNumCpus(3));

    /**
     * Test flyweighting an entire commodity bought.
     */
    @Test
    public void testFlyweightCommodityBought() {
        final TopologyEntityDTO e1 = entityBuilder()
            .addCommoditiesBoughtFromProviders(
                fromProvider(commodityBought.build()))
            .build();
        final TopologyEntityDTO e2 = entityBuilder()
            .addCommoditiesBoughtFromProviders(
                fromProvider(commodityBought.build()))
            .build();

        final TopologyEntityDTO e1Flyweight = flyweights.tryDeduplicate(e1);
        final TopologyEntityDTO e2Flyweight = flyweights.tryDeduplicate(e2);

        // The flyweighted entities should be equal to their originals.
        assertEquals(e1, e1Flyweight);
        assertEquals(e2, e2Flyweight);

        // In the flyweighted version, the commodity bought should now be reference-equal
        assertFalse(e1.getCommoditiesBoughtFromProviders(0).getCommodityBought(0)
            == e2.getCommoditiesBoughtFromProviders(0).getCommodityBought(0));
        assertTrue(e1Flyweight.getCommoditiesBoughtFromProviders(0).getCommodityBought(0)
            == e2Flyweight.getCommoditiesBoughtFromProviders(0).getCommodityBought(0));
    }

    /**
     * Test partially flyweighting the commodity bought.
     */
    @Test
    public void testPartialFlyweightCommodityBought() {
        final TopologyEntityDTO e1 = entityBuilder()
            .addCommoditiesBoughtFromProviders(
                fromProvider(commodityBought.build()))
            .build();
        final TopologyEntityDTO e2 = entityBuilder()
            .addCommoditiesBoughtFromProviders(
                fromProvider(commodityBought.setUsed(100.0f).build()))
            .build();

        final TopologyEntityDTO e1Flyweight = flyweights.tryDeduplicate(e1);
        final TopologyEntityDTO e2Flyweight = flyweights.tryDeduplicate(e2);

        // The flyweighted entities should be equal to their originals.
        assertEquals(e1, e1Flyweight);
        assertEquals(e2, e2Flyweight);

        // In the flyweighted version, the commodity bought should NOT be reference-equal
        // because they are not identical
        assertFalse(e1.getCommoditiesBoughtFromProviders(0).getCommodityBought(0)
            == e2.getCommoditiesBoughtFromProviders(0).getCommodityBought(0));
        final CommodityBoughtDTO c1 = e1Flyweight.getCommoditiesBoughtFromProviders(0).getCommodityBought(0);
        final CommodityBoughtDTO c2 = e2Flyweight.getCommoditiesBoughtFromProviders(0).getCommodityBought(0);
        assertFalse(c1 == c2);

        // but its sub-elements should be
        assertTrue(c1.getHistoricalUsed() == c2.getHistoricalUsed());
        assertTrue(c1.getHistoricalPeak() == c2.getHistoricalPeak());
        assertTrue(c1.getCommodityType() == c2.getCommodityType());
    }

    /**
     * Test flyweighting an entire commodity sold.
     */
    @Test
    public void testFlyweightCommoditySold() {
        final TopologyEntityDTO e1 = entityBuilder()
            .addCommoditySoldList(commoditySold)
            .build();
        final TopologyEntityDTO e2 = entityBuilder()
            .addCommoditySoldList(commoditySold)
            .build();

        final TopologyEntityDTO e1Flyweight = flyweights.tryDeduplicate(e1);
        final TopologyEntityDTO e2Flyweight = flyweights.tryDeduplicate(e2);

        // The original entities and their flyweighted version should be equal
        assertEquals(e1, e1Flyweight);
        assertEquals(e2, e2Flyweight);

        // In the flyweighted version, the commodity sold should now be reference-equal
        assertFalse(e1.getCommoditySoldList(0) == e2.getCommoditySoldList(0));
        assertTrue(e1Flyweight.getCommoditySoldList(0) == e2Flyweight.getCommoditySoldList(0));
    }

    /**
     * Test partially flyweighting commodity sold.
     */
    @Test
    public void testPartialFlyweightCommoditySold() {
        final TopologyEntityDTO e1 = entityBuilder()
            .addCommoditySoldList(commoditySold)
            .build();
        final TopologyEntityDTO e2 = entityBuilder()
            .addCommoditySoldList(commoditySold.setIsResold(false))
            .build();

        final TopologyEntityDTO e1Flyweight = flyweights.tryDeduplicate(e1);
        final TopologyEntityDTO e2Flyweight = flyweights.tryDeduplicate(e2);

        // The original entities and their flyweighted version should be equal
        assertEquals(e1, e1Flyweight);
        assertEquals(e2, e2Flyweight);

        // In the flyweighted version, the commodity sold could not be flyweighted because they are different
        final CommoditySoldDTO c1 = e1Flyweight.getCommoditySoldList(0);
        final CommoditySoldDTO c2 = e2Flyweight.getCommoditySoldList(0);
        assertFalse(e1.getCommoditySoldList(0) == e2.getCommoditySoldList(0));
        assertFalse(c1 == c2);

        // But all its sub-elements should be flyweighted
        assertTrue(c1.getHistoricalUsed() == c2.getHistoricalUsed());
        assertTrue(c1.getHistoricalPeak() == c2.getHistoricalPeak());
        assertTrue(c1.getThresholds() == c2.getThresholds());
        assertTrue(c1.getCommodityType() == c2.getCommodityType());
        assertTrue(c1.getHotResizeInfo() == c2.getHotResizeInfo());
    }


    /**
     * Test flyweighting a connection.
     */
    @Test
    public void testFlyweightConnection() {
        final TopologyEntityDTO e1 = entityBuilder()
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                .setConnectedEntityId(3456L))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                .setConnectedEntityId(6789L)
            ).build();
        final TopologyEntityDTO e2 = entityBuilder().addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                .setConnectedEntityId(6789L)
        ).build();

        final TopologyEntityDTO e1Flyweight = flyweights.tryDeduplicate(e1);
        final TopologyEntityDTO e2Flyweight = flyweights.tryDeduplicate(e2);

        // The original entities and their flyweight version should be equal
        assertEquals(e1, e1Flyweight);
        assertEquals(e2, e2Flyweight);

        // In the flyweighted version, the duplicate connections should now be reference-equal
        assertFalse(e1.getConnectedEntityList(1) == e2.getConnectedEntityList(0));
        assertTrue(e1Flyweight.getConnectedEntityList(1) == e2Flyweight.getConnectedEntityList(0));
    }

    /**
     * Test flyweighting of tags.
     */
    @Test
    public void testFlyweightTags() {
        final TopologyEntityDTO e1 = entityBuilder()
            .setTags(tags)
            .build();
        final TopologyEntityDTO e2 = entityBuilder()
            .setTags(tags)
            .build();

        final TopologyEntityDTO e1Flyweight = flyweights.tryDeduplicate(e1);
        final TopologyEntityDTO e2Flyweight = flyweights.tryDeduplicate(e2);

        // The original entities and their flyweighted version should be equal
        assertEquals(e1, e1Flyweight);
        assertEquals(e2, e2Flyweight);

        // In the flyweighted version, the commodity sold should now be reference-equal
        assertFalse(e1.getTags() == e2.getTags());
        assertTrue(e1Flyweight.getTags() == e2Flyweight.getTags());
    }

    /**
     * Test flyweighting of analysis settings.
     */
    @Test
    public void testFlyweightAnalysisSettings() {
        final TopologyEntityDTO e1 = entityBuilder()
            .setAnalysisSettings(analysisSettings)
            .build();
        final TopologyEntityDTO e2 = entityBuilder()
            .setAnalysisSettings(analysisSettings)
            .build();

        final TopologyEntityDTO e1Flyweight = flyweights.tryDeduplicate(e1);
        final TopologyEntityDTO e2Flyweight = flyweights.tryDeduplicate(e2);

        // The original entities and their flyweighted version should be equal
        assertEquals(e1, e1Flyweight);
        assertEquals(e2, e2Flyweight);

        // In the flyweighted version, the commodity sold should now be reference-equal
        assertFalse(e1.getAnalysisSettings() == e2.getAnalysisSettings());
        assertTrue(e1Flyweight.getAnalysisSettings() == e2Flyweight.getAnalysisSettings());
    }

    /**
     * Test flyweighting of type specific info.
     */
    @Test
    public void testFlyweightTypeSpecificInfo() {
        final TopologyEntityDTO e1 = entityBuilder()
            .setTypeSpecificInfo(tsInfo)
            .build();
        final TopologyEntityDTO e2 = entityBuilder()
            .setTypeSpecificInfo(tsInfo)
            .build();

        final TopologyEntityDTO e1Flyweight = flyweights.tryDeduplicate(e1);
        final TopologyEntityDTO e2Flyweight = flyweights.tryDeduplicate(e2);

        // The original entities and their flyweighted version should be equal
        assertEquals(e1, e1Flyweight);
        assertEquals(e2, e2Flyweight);

        // In the flyweighted version, the commodity sold should now be reference-equal
        assertFalse(e1.getTypeSpecificInfo() == e2.getTypeSpecificInfo());
        assertTrue(e1Flyweight.getTypeSpecificInfo() == e2Flyweight.getTypeSpecificInfo());
    }

    private CommoditiesBoughtFromProvider fromProvider(@Nonnull final CommodityBoughtDTO bought) {
        return CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(2345L)
            .addCommodityBought(bought)
            .build();
    }

    private TopologyEntityDTO.Builder entityBuilder() {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(1234L);
    }
}