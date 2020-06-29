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
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
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
    private final CommoditySoldDTO sourceCommodity;
    private final CommodityType sourceCommodityType;

    private final double sourceCapacity = 250;

    private final CommoditySoldDTO originalAllocationCommodity;
    private final double expectedAllocationCapacity = sourceCapacity * overprovisionPercentage / 100;
    private final CommodityType allocationCommodityType;

    private final List<CommoditySoldDTO> requiredCommodities;
    private final List<CommoditySoldDTO> expectedCommodities;

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

    private final CommoditySoldDTO irrelevantCommodity = makeCommoditySold(CommodityType.BALLOONING);
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
        final TopologyEntity testTE = makeTopologyEntity(Collections.emptyList());
        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoSettings() {
        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
            eq(overprovisionSettingType))).thenReturn(Optional.empty());

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);
        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().size() == 1);
    }

    @Test
    public void testNoSourceCommodity() {

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(originalAllocationCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoAllocationCommodity() {

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testAllocationCommodityWithCapacity() {

        final CommoditySoldDTO preloadedAllocation = makeCommoditySold(allocationCommodityType, 99);
        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(sourceCommodity, preloadedAllocation);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        // check that there are no changes, because the capacity was already set
        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testAllocationCommodityWithoutCapacity() {

        final CommoditySoldDTO preloadedAllocation = CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(allocationCommodityType.getNumber()))
                .build();

        final List<CommoditySoldDTO> origCommodities =
                Arrays.asList(sourceCommodity, preloadedAllocation);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        // run the operation and apply changes
        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        // check that the capacity is filled with the expected value
        assertThat(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
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

        final CommoditySoldDTO allocationWithKey = makeCommoditySold(allocationCommodityType, key1);
        final CommoditySoldDTO sourceWithKey = makeCommoditySold(sourceCommodityType, sourceCapacity, key2);

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(sourceWithKey, allocationWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        // Keys get ignored for the PM Allocation operations.
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertThat(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
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

        final CommoditySoldDTO dogCommodity = makeCommoditySold(allocationCommodityType, dogKey);
        final CommoditySoldDTO catCommodity = makeCommoditySold(allocationCommodityType, catKey);
        final TopologyEntity testTE =
                makeTopologyEntity(Arrays.asList(dogCommodity, catCommodity, sourceCommodity));

        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertThat(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
                containsInAnyOrder(sourceCommodity,
                    dogCommodity.toBuilder().setCapacity(expectedAllocationCapacity).build(),
                    catCommodity.toBuilder().setCapacity(expectedAllocationCapacity).build()));
    }

    /**
     * Test that if there are multiple source commodities with different keys
     * we use the first encountered one.
     */
    @Test
    public void testDiffKeySourceCommodities() {
        final CommoditySoldDTO fooSource = makeCommoditySold(sourceCommodityType, sourceCapacity, "foo");
        final CommoditySoldDTO barSource = makeCommoditySold(sourceCommodityType, 99, "bar");
        // No key necessary, since we ignore keys anyway.
        final CommoditySoldDTO allocationCommodity = makeCommoditySold(allocationCommodityType);
        final TopologyEntity testTE =
                makeTopologyEntity(Arrays.asList(fooSource, barSource, allocationCommodity));

        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertThat(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
                containsInAnyOrder(fooSource, barSource,
                        // Allocation commodity should have the expected allocation capacity
                        // derived from sourceCapacity.
                        makeCommoditySold(allocationCommodityType, expectedAllocationCapacity)));
    }

    @Test
    public void testDuplicateCommodities() {

        final List<CommoditySoldDTO> origCommodities = new ArrayList<>();
        final CommoditySoldDTO duplicateCommodity = makeCommoditySold(sourceCommodityType, 99);
        origCommodities.addAll(requiredCommodities);
        origCommodities.add(duplicateCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

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

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        final List<CommoditySoldDTO> actualCommodities =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }

    @Test
    public void testHappyPathWithKeys() {

        final String key = "abcdefghij";
        final CommoditySoldDTO sourceCommodityWithKey =
            makeCommoditySold(sourceCommodityType, sourceCapacity, key);

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(allocationCommodityType, key), sourceCommodityWithKey);

        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        final List<CommoditySoldDTO> actualCommodities =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        final List<CommoditySoldDTO> expectedCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(allocationCommodityType, expectedAllocationCapacity, key),
            sourceCommodityWithKey);

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }

    @Test
    public void testMultipleEntities() {

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        final double secondCapacity = 500;
        final CommoditySoldDTO secondSourceCommodity = makeCommoditySold(sourceCommodityType, secondCapacity);
        final TopologyEntity secondTestTE =
            makeTopologyEntity(Arrays.asList(secondSourceCommodity, originalAllocationCommodity));

        final List<CommoditySoldDTO> thirdCommodityList =
            Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity thirdTestTE = makeTopologyEntity(thirdCommodityList);

        final CommoditySoldDTO commodityWithCapacity = makeCommoditySold(allocationCommodityType, 4);
        final List<CommoditySoldDTO> fourthCommodityList =
            Arrays.asList(sourceCommodity, commodityWithCapacity, irrelevantCommodity);
        final TopologyEntity fourthTestTE = makeTopologyEntity(fourthCommodityList);

        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(Stream.of(testTE,
            secondTestTE, thirdTestTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        final List<CommoditySoldDTO> firstResult =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        assertTrue(firstResult.containsAll(expectedCommodities));
        assertTrue(expectedCommodities.containsAll(firstResult));

        final List<CommoditySoldDTO> secondExpectedResult = Arrays.asList(secondSourceCommodity,
            makeCommoditySold(allocationCommodityType, secondCapacity * overprovisionPercentage / 100));
        assertEquals(secondTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            secondExpectedResult);

        assertEquals(thirdTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            thirdCommodityList);
        assertEquals(fourthTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            fourthCommodityList);
    }
}


