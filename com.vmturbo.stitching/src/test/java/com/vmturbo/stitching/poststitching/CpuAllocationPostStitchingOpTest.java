package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeNumericSetting;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class CpuAllocationPostStitchingOpTest {


    private final CpuAllocationPostStitchingOperation operation =
        new CpuAllocationPostStitchingOperation();
    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final CommoditySoldDTO irrelevantCommodity = makeCommoditySold(CommodityType.BALLOONING);

    private final float overprovisionPercentage = 150;


    private final double sourceCapacity = 250;
    private final CommoditySoldDTO cpuCommodity = makeCommoditySold(CommodityType.CPU, sourceCapacity);

    private final CommoditySoldDTO originalAllocationCommodity = makeCommoditySold(CommodityType.CPU_ALLOCATION);
    private final double expectedAllocationCapacity = sourceCapacity * overprovisionPercentage / 100;


    private final List<CommoditySoldDTO> requiredCommodities = ImmutableList.of(originalAllocationCommodity, cpuCommodity, irrelevantCommodity);
    private final List<CommoditySoldDTO> expectedCommodities = ImmutableList.of(cpuCommodity, irrelevantCommodity,
        makeCommoditySold(CommodityType.CPU_ALLOCATION, expectedAllocationCapacity));




    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();

        final Setting overprovisionSetting = makeNumericSetting(overprovisionPercentage);
        when(settingsMock.getEntitySetting(any(TopologyEntity.class), eq(EntitySettingSpecs.CpuOverprovisionedPercentage)))
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

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            Collections.emptyList());

    }

    @Test
    public void testNoSettings() {
        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
            eq(EntitySettingSpecs.CpuOverprovisionedPercentage))).thenReturn(Optional.empty());

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), requiredCommodities);
    }

    @Test
    public void testNoSourceCommodity() {

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(originalAllocationCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        final List<CommoditySoldDTO> resultCommodities =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();

        assertTrue(origCommodities.containsAll(resultCommodities));
        assertTrue(resultCommodities.containsAll(origCommodities));
    }

    @Test
    public void testNoAllocationCommodity() {

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(cpuCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), origCommodities);
    }

    @Test
    public void testAllocationCommodityWithCapacity() {

        final CommoditySoldDTO preloadedAllocation = makeCommoditySold(CommodityType.CPU_ALLOCATION, 99);
        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(cpuCommodity, irrelevantCommodity, preloadedAllocation);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), origCommodities);
    }

    @Test
    public void testMismatchingKeys() {

        final String key1 = "abc";
        final String key2 = "xyz";

        final CommoditySoldDTO allocationWithKey = makeCommoditySold(CommodityType.CPU_ALLOCATION, key1);
        final CommoditySoldDTO cpuWithKey = makeCommoditySold(CommodityType.CPU, sourceCapacity, key2);

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(cpuWithKey, irrelevantCommodity, allocationWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), origCommodities);
    }

    @Test
    public void testOneHasKeyOtherDoesnt() {

        final String key = "abc";

        final CommoditySoldDTO allocationWithKey = makeCommoditySold(CommodityType.CPU_ALLOCATION, key);

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(cpuCommodity, irrelevantCommodity, allocationWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), origCommodities);
    }

    @Test
    public void testDuplicateCommodities() {

        final List<CommoditySoldDTO> origCommodities = new ArrayList<>();
        final CommoditySoldDTO duplicateCommodity = makeCommoditySold(CommodityType.CPU, sourceCapacity);
        origCommodities.addAll(requiredCommodities);
        origCommodities.add(duplicateCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        try {
            final TopologicalChangelog result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
            result.getChanges().forEach(TopologicalChange::applyChange);
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Found multiple commodities of type " +
                CommodityType.CPU +" with key "+ duplicateCommodity.getCommodityType().getKey() +
                " in entity 0");
        }
    }

    @Test
    public void testHappyPathNoKeys() {

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        final List<CommoditySoldDTO> actualCommodities =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }

    @Test
    public void testHappyPathWithKeys() {

        final String key = "abcdefghij";
        final CommoditySoldDTO cpuCommodityWithKey =
            makeCommoditySold(CommodityType.CPU, sourceCapacity, key);

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(CommodityType.CPU_ALLOCATION, key), cpuCommodityWithKey);

        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        final List<CommoditySoldDTO> actualCommodities =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        final List<CommoditySoldDTO> expectedCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(CommodityType.CPU_ALLOCATION, expectedAllocationCapacity, key),
            cpuCommodityWithKey);

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }

    @Test
    public void testMultipleEntities() {

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        final double secondCapacity = 500;
        final CommoditySoldDTO secondSourceCommodity = makeCommoditySold(CommodityType.CPU, secondCapacity);
        final TopologyEntity secondTestTE =
            makeTopologyEntity(Arrays.asList(secondSourceCommodity, originalAllocationCommodity));

        final List<CommoditySoldDTO> thirdCommodityList =
            Arrays.asList(cpuCommodity, irrelevantCommodity);
        final TopologyEntity thirdTestTE = makeTopologyEntity(thirdCommodityList);

        final CommoditySoldDTO commodityWithCapacity = makeCommoditySold(CommodityType.CPU_ALLOCATION, 4);
        final List<CommoditySoldDTO> fourthCommodityList =
            Arrays.asList(cpuCommodity, commodityWithCapacity, irrelevantCommodity);
        final TopologyEntity fourthTestTE = makeTopologyEntity(fourthCommodityList);

        final TopologicalChangelog result = operation.performOperation(Stream.of(testTE,
            secondTestTE, thirdTestTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        final List<CommoditySoldDTO> firstResult =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        assertTrue(firstResult.containsAll(expectedCommodities));
        assertTrue(expectedCommodities.containsAll(firstResult));

        final List<CommoditySoldDTO> secondExpectedResult = Arrays.asList(secondSourceCommodity,
            makeCommoditySold(CommodityType.CPU_ALLOCATION, secondCapacity * overprovisionPercentage / 100));
        assertEquals(secondTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            secondExpectedResult);

        assertEquals(thirdTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            thirdCommodityList);
        assertEquals(fourthTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            fourthCommodityList);
    }
}


