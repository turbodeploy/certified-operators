package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBought;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class VirtualDatacenterCpuAllocationPostStitchingOpTest {

    private final VirtualDatacenterCpuAllocationPostStitchingOperation op =
        new VirtualDatacenterCpuAllocationPostStitchingOperation();

    private static final double hostCapacity = 20;

    private final CommoditySoldDTO baseEmptyCommoditySold = makeCommoditySold(CommodityType.CPU_ALLOCATION);
    private final CommoditySoldDTO baseFullCommoditySold =
        makeCommoditySold(CommodityType.CPU_ALLOCATION, hostCapacity, "abcdef");

    private final CommodityBoughtDTO baseCommodityBought =
        makeCommodityBought(CommodityType.CPU_ALLOCATION, "abcdef");

    private final TopologyEntity.Builder basicHostProvider =
        makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseFullCommoditySold), Collections.emptyList());
    private final TopologyEntity.Builder secondHostProvider =
                    makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
                                    Collections.singletonList(baseFullCommoditySold), Collections.emptyList());

    private final TopologyEntity.Builder basicLayerProvider =
        makeTopologyEntityBuilder(EntityType.PHYSICAL_MACHINE_VALUE,
            Collections.singletonList(baseFullCommoditySold), Collections.emptyList());

    private final EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> stitchingJournal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);
    
    private UnitTestResultBuilder resultBuilder;

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
    }

    @Test
    public void testNoEntities() {
        op.performOperation(Stream.empty(), settingsCollection, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityWithNoCommoditiesSold() {
        final TopologyEntity te = makeTopologyEntity(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.emptyList(), Collections.singletonList(baseCommodityBought),
            Collections.singletonList(basicHostProvider));
        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testEntityWithOnlyUnusableCommoditiesSold() {
        final CommoditySoldDTO wrong1 = makeCommoditySold(CommodityType.STORAGE_LATENCY);
        final CommoditySoldDTO wrong2 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 20);

        final TopologyEntity te = makeTopologyEntity(EntityType.VIRTUAL_DATACENTER_VALUE,
            Arrays.asList(wrong1, wrong2), Collections.singletonList(baseCommodityBought),
            Collections.singletonList(basicHostProvider));

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testConsumerWithMultipleHosts() {

        final TopologyEntity.Builder base =
            makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
                Collections.singletonList(baseEmptyCommoditySold), Collections.emptyList());
        base.addProvider(basicHostProvider);
        base.addProvider(secondHostProvider);

        final TopologyEntity te = base.build();
        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));
        Assert.assertThat(te.getTopologyEntityDtoBuilder().getCommoditySoldListList().iterator()
                        .next().getCapacity(), CoreMatchers.is(20D));
    }

    @Test
    public void testConsumerHostHasNoUsableCommodities() {
        final TopologyEntity.Builder hostWithNoGoodCommodities =
            makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
                Collections.singletonList(baseEmptyCommoditySold), Collections.emptyList());
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold), Collections.emptyList());
        main.addProvider(hostWithNoGoodCommodities);

        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testConsumerHostHasMultipleQualifyingCommodities() {
        final TopologyEntity.Builder hostWithNoGoodCommodities =
            makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
                Arrays.asList(makeCommoditySold(CommodityType.CPU_ALLOCATION, hostCapacity, "abcdefj"), baseFullCommoditySold), Collections.emptyList());
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold), Collections.emptyList());
        main.addProvider(hostWithNoGoodCommodities);

        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testConsumerHappyPath() {
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold), Collections.emptyList());
        main.addProvider(basicHostProvider);

        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), hostCapacity, 0.1));
    }

    @Test
    public void testProducerHasNoBoughtCommodities() {
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold), Collections.emptyList());
        main.addProvider(basicLayerProvider);
        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(resultBuilder.getChanges().size(), 0);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 0, 0.1));
    }

    @Test
    public void testNoProvidersMeansProducerWithEmptyCapacity() {

        final List<CommoditySoldDTO> commoditiesSold = Collections.singletonList(baseEmptyCommoditySold);

        final List<CommodityBoughtDTO> commoditiesBought = Collections.singletonList(baseCommodityBought);

        final TopologyEntity te = makeTopologyEntity(EntityType.VIRTUAL_DATACENTER_VALUE,
            commoditiesSold, commoditiesBought, Collections.emptyList());

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 0, 0.1));
    }

    @Test
    public void testHappyPath() {
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold), Collections.singletonList(baseCommodityBought));
        main.addProvider(basicLayerProvider);

        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(commodity ->
            assertEquals(commodity.getCapacity(), hostCapacity, 0.1));
    }

    @Test
    public void testDuplicatesBought() {
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold),
            Arrays.asList(baseCommodityBought, baseCommodityBought));
        main.addProvider(basicLayerProvider);

        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(commodity ->
            assertEquals(commodity.getCapacity(), hostCapacity, 0.1));
    }

    @Test
    public void testMultipleBought() {

        final CommodityBoughtDTO secondCommodityBought =
            makeCommodityBought(CommodityType.CPU_ALLOCATION, "12345");
        final CommoditySoldDTO secondCommoditySold = makeCommoditySold(CommodityType.CPU_ALLOCATION, 215, "12345");
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold),
            Arrays.asList(baseCommodityBought, secondCommodityBought));

        final TopologyEntity.Builder multiProvider =
            makeTopologyEntityBuilder(EntityType.PHYSICAL_MACHINE_VALUE,
                Arrays.asList(baseFullCommoditySold, secondCommoditySold), Collections.emptyList());
        main.addProvider(multiProvider);

        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(resultBuilder.getChanges().size(), 1);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(commodity ->
            assertEquals(commodity.getCapacity(), hostCapacity + 215, 0.1));
    }

    @Test
    public void testUnmatchingCommodityKey() {
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold),
            Collections.singletonList(makeCommodityBought(CommodityType.CPU_ALLOCATION)));
        main.addProvider(basicLayerProvider);
        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(resultBuilder.getChanges().size(), 0);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 0, 0.1));
    }

    /**
     * Checks that in case there is more than one VDC in hierarchy without capacity, then capacity
     * will be set to all VDCs in hierarchy.
     */
    @Test
    public void checkVdcHierarchyWithoutCapacity() {
        final TopologyEntity.Builder leaf = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
                        Collections.singletonList(baseEmptyCommoditySold),
                        Collections.singletonList(makeCommodityBought(CommodityType.CPU_ALLOCATION)));
        leaf.getEntityBuilder().setOid(1);
        final TopologyEntity.Builder root = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
                        Collections.singletonList(baseEmptyCommoditySold), Collections.singletonList(baseCommodityBought));
        root.getEntityBuilder().setOid(2);
        leaf.addProvider(root);
        root.addProvider(basicLayerProvider);
        final TopologyEntity te = leaf.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(resultBuilder.getChanges().size(), 2);
        Assert.assertThat(getCapacity(te), CoreMatchers.is(hostCapacity));
        Assert.assertThat(getCapacity(root.build()), CoreMatchers.is(hostCapacity));
    }

    private static double getCapacity(TopologyEntity te) {
        return te.getTopologyEntityDtoBuilder().getCommoditySoldListList().iterator().next()
                        .getCapacity();
    }

    @Test
    public void testProviderNoCommoditiesSold() {
        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold),
            Collections.singletonList(makeCommodityBought(CommodityType.CPU_ALLOCATION)));
        final TopologyEntity.Builder emptyProvider = makeTopologyEntityBuilder(EntityType.PHYSICAL_MACHINE_VALUE,
            Collections.emptyList(), Collections.emptyList());
        main.addProvider(emptyProvider);
        final TopologyEntity te = main.build();

        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        assertEquals(resultBuilder.getChanges().size(), 0);
        te.getTopologyEntityDtoBuilder().getCommoditySoldListList()
            .forEach(commodity -> assertEquals(commodity.getCapacity(), 0, 0.1));
    }

    @Test
    public void testProducerWithDuplicates() {
        final CommoditySoldDTO sold1 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 1, "abc");
        final CommoditySoldDTO sold2 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 10000, "abc");
        final CommoditySoldDTO sold3 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 2, "def");
        final CommoditySoldDTO sold4 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 3);

        final CommodityBoughtDTO bought1 = makeCommodityBought(CommodityType.CPU_ALLOCATION, "abc");
        final CommodityBoughtDTO bought2 = makeCommodityBought(CommodityType.CPU_ALLOCATION, "def");
        final CommodityBoughtDTO bought3 = makeCommodityBought(CommodityType.CPU_ALLOCATION);
        final CommodityBoughtDTO bought4 = makeCommodityBought(CommodityType.CPU_ALLOCATION);

        final TopologyEntity.Builder provider =
            makeTopologyEntityBuilder(EntityType.PHYSICAL_MACHINE_VALUE,
                Arrays.asList(sold1, sold2, sold3, sold4), Collections.emptyList());
        final TopologyEntity.Builder base =
            makeTopologyEntityBuilder(
                EntityType.VIRTUAL_DATACENTER_VALUE,
                Collections.singletonList(makeCommoditySold(CommodityType.CPU_ALLOCATION)),
                Arrays.asList(bought1, bought2, bought3, bought4));
        base.addProvider(provider);
        final TopologyEntity main = base.build();

        op.performOperation(Stream.of(main), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        // Expected is 6 because: 1 + 2 + 3 = 6
        Assert.assertThat(main.getTopologyEntityDtoBuilder().getCommoditySoldListList().iterator()
                        .next().getCapacity(), CoreMatchers.is(6D));
    }

    @Test
    public void testMultiProviderSameCommodities() {

        final CommodityBoughtDTO bought1 = makeCommodityBought(CommodityType.CPU_ALLOCATION, "asdf");
        final CommodityBoughtDTO bought2 = makeCommodityBought(CommodityType.CPU_ALLOCATION, "jkl;");
        final CommodityBoughtDTO bought3 = makeCommodityBought(CommodityType.CPU_ALLOCATION, "qwerty");


        final CommoditySoldDTO sold1 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 20, "asdf");
        final CommoditySoldDTO sold2 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 50, "jkl;");
        final CommoditySoldDTO sold3 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 100, "qwerty");
        final TopologyEntity.Builder provider1 = makeTopologyEntityBuilder(EntityType.PHYSICAL_MACHINE_VALUE,
            Arrays.asList(sold1, sold2, sold3), Collections.emptyList());
        final TopologyEntity.Builder provider2 = makeTopologyEntityBuilder(EntityType.PHYSICAL_MACHINE_VALUE,
            Arrays.asList(sold1, sold2, sold3), Collections.emptyList());

        final TopologyEntity.Builder main = makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
            Collections.singletonList(baseEmptyCommoditySold), Arrays.asList(bought1, bought2, bought3));
        main.addProvider(provider1);
        main.addProvider(provider2);

        final TopologyEntity te = main.build();


        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        te.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(cs ->
            assertEquals(cs.getCapacity(), 340, .1));
    }

    @Test
    public void testMultiProviderDifferentCommodities() {

        final CommodityBoughtDTO bought1 = makeCommodityBought(CommodityType.CPU_ALLOCATION, "asdf");
        final CommodityBoughtDTO bought2 = makeCommodityBought(CommodityType.CPU_ALLOCATION, "jkl;");
        final CommodityBoughtDTO bought3 = makeCommodityBought(CommodityType.CPU_ALLOCATION, "qwerty");


        final CommoditySoldDTO sold1 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 20, "asdf");
        final CommoditySoldDTO sold2 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 50, "jkl;");
        final CommoditySoldDTO sold3 = makeCommoditySold(CommodityType.CPU_ALLOCATION, 100, "qwerty");
        final TopologyEntity.Builder provider1 =
            makeTopologyEntityBuilder(EntityType.PHYSICAL_MACHINE_VALUE,
                Arrays.asList(sold1, sold3), Collections.emptyList());
        final TopologyEntity.Builder provider2 =
            makeTopologyEntityBuilder(EntityType.PHYSICAL_MACHINE_VALUE,
                Arrays.asList(sold2, sold3), Collections.emptyList());

        final TopologyEntity.Builder main =
            makeTopologyEntityBuilder(EntityType.VIRTUAL_DATACENTER_VALUE,
                Collections.singletonList(baseEmptyCommoditySold),
                Arrays.asList(bought1, bought2, bought3));
        main.addProvider(provider1);
        main.addProvider(provider2);

        final TopologyEntity te = main.build();


        op.performOperation(Stream.of(te), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        te.getTopologyEntityDtoBuilder().getCommoditySoldListList().forEach(cs ->
            assertEquals(cs.getCapacity(), 270, .1));
    }
}
