package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeNumericSetting;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.PmCpuAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.PmMemoryAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

@RunWith(Parameterized.class)
public class PmAllocationPostStitchingOpTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {new PmCpuAllocationPostStitchingOperation(), CommodityType.CPU,
                CommodityType.CPU_ALLOCATION, EntitySettingSpecs.CpuOverprovisionedPercentage},
            {new PmMemoryAllocationPostStitchingOperation(), CommodityType.MEM,
                CommodityType.MEM_ALLOCATION, EntitySettingSpecs.MemoryOverprovisionedPercentage}
        });
    }

    private final OverprovisionCapacityPostStitchingOperation operation;

    private final float overprovisionPercentage = 150;
    private final EntitySettingSpecs overprovisionSettingType;
    private final CommoditySoldView sourceCommodity;
    private final CommodityType sourceCommodityType;

    private final double sourceCapacity = 250;

    private final CommoditySoldView originalAllocationCommodity;
    private final double expectedAllocationCapacity = sourceCapacity * overprovisionPercentage / 100;
    private final CommodityType allocationCommodityType;

    private final List<CommoditySoldView> requiredCommodities;
    private final List<CommoditySoldView> expectedCommodities;

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    public PmAllocationPostStitchingOpTest(@Nonnull final OverprovisionCapacityPostStitchingOperation op,
                                           @Nonnull final CommodityType sourceType,
                                           @Nonnull final CommodityType allocationType,
                                           @Nonnull final EntitySettingSpecs settingType) {
        this.operation = op;
        this.sourceCommodityType = sourceType;
        this.sourceCommodity = makeCommoditySold(sourceCommodityType, sourceCapacity);
        this.allocationCommodityType = allocationType;
        this.originalAllocationCommodity = makeCommoditySold(allocationCommodityType);

        this.overprovisionSettingType = settingType;

        this.requiredCommodities =
            ImmutableList.of(originalAllocationCommodity, sourceCommodity, irrelevantCommodity);
        this.expectedCommodities = ImmutableList.of(sourceCommodity, irrelevantCommodity,
            makeCommoditySold(allocationCommodityType, expectedAllocationCapacity));
    }

    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final CommoditySoldView irrelevantCommodity = makeCommoditySold(CommodityType.BALLOONING);
    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);
    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> stitchingJournal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();

        final Setting overprovisionSetting = makeNumericSetting(overprovisionPercentage);
        when(settingsMock.getEntitySetting(any(TopologyEntity.class), eq(overprovisionSettingType)))
            .thenReturn(Optional.of(overprovisionSetting));

    }

    @Test
    public void testNoEntities() {
        final TopologicalChangelog result =
            operation.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    @Test
    public void testNoCommodities() {
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(Collections.emptyList());
        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoSettings() {
        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
            eq(overprovisionSettingType))).thenReturn(Optional.empty());

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(requiredCommodities);
        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().size() == 1);
    }

    @Test
    public void testNoSourceCommodity() {

        final List<CommoditySoldView> origCommodities =
            Arrays.asList(originalAllocationCommodity, irrelevantCommodity);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoAllocationCommodity() {

        final List<CommoditySoldView> origCommodities = Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testAllocationCommodityWithCapacity() {

        final CommoditySoldView preloadedAllocation = makeCommoditySold(allocationCommodityType, 99);
        final List<CommoditySoldView> origCommodities =
            Arrays.asList(sourceCommodity, preloadedAllocation);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        // check that there are no changes, because the capacity was already set
        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testAllocationCommodityWithoutCapacity() {

        final CommoditySoldView preloadedAllocation = new CommoditySoldImpl()
                .setCommodityType(new CommodityTypeImpl()
                .setType(allocationCommodityType.getNumber()));

        final List<CommoditySoldView> origCommodities =
                Arrays.asList(sourceCommodity, preloadedAllocation);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        // run the operation and apply changes
        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        // check that the capacity is filled with the expected value
        assertThat(testTE.getTopologyEntityImpl().getCommoditySoldListList(),
                containsInAnyOrder(sourceCommodity,
                        makeCommoditySold(allocationCommodityType, expectedAllocationCapacity)));
    }

    /**
     * Test that the allocation change gets applied even when keys are not matching.
     */
    @Test
    public void testMismatchingKeys() {

        final String key1 = "abc";
        final String key2 = "xyz";

        final CommoditySoldView allocationWithKey = makeCommoditySold(allocationCommodityType, key1);
        final CommoditySoldView sourceWithKey = makeCommoditySold(sourceCommodityType, sourceCapacity, key2);

        final List<CommoditySoldView> origCommodities =
            Arrays.asList(sourceWithKey, allocationWithKey);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        // Keys get ignored for the PM Allocation operations.
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertThat(testTE.getTopologyEntityImpl().getCommoditySoldListList(),
                containsInAnyOrder(sourceWithKey, makeCommoditySold(allocationCommodityType, expectedAllocationCapacity, key1)));
    }

    /**
     * Test that a single source commodity gets used to compute the capacities of all matching
     * allocation commodities, regardless of key.
     */
    @Test
    public void testMultipleAllocationCommoditiesDifferentKeys() {
        final String dogKey = "dog";
        final String catKey = "cat";

        final CommoditySoldView dogCommodity = makeCommoditySold(allocationCommodityType, dogKey);
        final CommoditySoldView catCommodity = makeCommoditySold(allocationCommodityType, catKey);
        final TopologyEntity testTE =
                PostStitchingTestUtilities.makeTopologyEntity(Arrays.asList(dogCommodity, catCommodity, sourceCommodity));

        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertThat(testTE.getTopologyEntityImpl().getCommoditySoldListList(),
                containsInAnyOrder(sourceCommodity,
                    dogCommodity.copy().setCapacity(expectedAllocationCapacity),
                    catCommodity.copy().setCapacity(expectedAllocationCapacity)));
    }

    /**
     * Test that if there are multiple source commodities with different keys
     * we use the first encountered one.
     */
    @Test
    public void testDiffKeySourceCommodities() {
        final CommoditySoldView fooSource = makeCommoditySold(sourceCommodityType, sourceCapacity, "foo");
        final CommoditySoldView barSource = makeCommoditySold(sourceCommodityType, 99, "bar");
        // No key necessary, since we ignore keys anyway.
        final CommoditySoldView allocationCommodity = makeCommoditySold(allocationCommodityType);
        final TopologyEntity testTE =
                PostStitchingTestUtilities.makeTopologyEntity(Arrays.asList(fooSource, barSource, allocationCommodity));

        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertThat(testTE.getTopologyEntityImpl().getCommoditySoldListList(),
                containsInAnyOrder(fooSource, barSource,
                        // Allocation commodity should have the expected allocation capacity
                        // derived from sourceCapacity.
                        makeCommoditySold(allocationCommodityType, expectedAllocationCapacity)));
    }

    @Test
    public void testDuplicateCommodities() {

        final List<CommoditySoldView> origCommodities = new ArrayList<>();
        final CommoditySoldView duplicateCommodity = makeCommoditySold(sourceCommodityType, 99);
        origCommodities.addAll(requiredCommodities);
        origCommodities.add(duplicateCommodity);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        try {
            final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
            result.getChanges().forEach(change -> change.applyChange(stitchingJournal));
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Found multiple commodities of type " +
                sourceCommodityType +" with key "+ duplicateCommodity.getCommodityType().getKey() +
                " in entity 0");
        }
    }

    @Test
    public void testHappyPathNoKeys() {

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(requiredCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        final List<CommoditySoldView> actualCommodities =
            testTE.getTopologyEntityImpl().getCommoditySoldListList();

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }

    @Test
    public void testHappyPathWithKeys() {

        final String key = "abcdefghij";
        final CommoditySoldView sourceCommodityWithKey =
            makeCommoditySold(sourceCommodityType, sourceCapacity, key);

        final List<CommoditySoldView> origCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(allocationCommodityType, key), sourceCommodityWithKey);

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        final List<CommoditySoldView> actualCommodities =
            testTE.getTopologyEntityImpl().getCommoditySoldListList();
        final List<CommoditySoldView> expectedCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(allocationCommodityType, expectedAllocationCapacity, key),
            sourceCommodityWithKey);

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }

    @Test
    public void testMultipleEntities() {

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(requiredCommodities);

        final double secondCapacity = 500;
        final CommoditySoldView secondSourceCommodity = makeCommoditySold(sourceCommodityType, secondCapacity);
        final TopologyEntity secondTestTE =
            PostStitchingTestUtilities.makeTopologyEntity(Arrays.asList(secondSourceCommodity, originalAllocationCommodity));

        final List<CommoditySoldView> thirdCommodityList =
            Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity thirdTestTE = PostStitchingTestUtilities.makeTopologyEntity(thirdCommodityList);

        final CommoditySoldView commodityWithCapacity = makeCommoditySold(allocationCommodityType, 4);
        final List<CommoditySoldView> fourthCommodityList =
            Arrays.asList(sourceCommodity, commodityWithCapacity, irrelevantCommodity);
        final TopologyEntity fourthTestTE = PostStitchingTestUtilities.makeTopologyEntity(fourthCommodityList);

        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(Stream.of(testTE,
            secondTestTE, thirdTestTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        final List<CommoditySoldView> firstResult =
            testTE.getTopologyEntityImpl().getCommoditySoldListList();
        assertTrue(firstResult.containsAll(expectedCommodities));
        assertTrue(expectedCommodities.containsAll(firstResult));

        final List<CommoditySoldView> secondExpectedResult = Arrays.asList(secondSourceCommodity,
            makeCommoditySold(allocationCommodityType, secondCapacity * overprovisionPercentage / 100));
        assertEquals(secondTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
            secondExpectedResult);

        assertEquals(thirdTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
            thirdCommodityList);
        assertEquals(fourthTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
            fourthCommodityList);
    }
}


