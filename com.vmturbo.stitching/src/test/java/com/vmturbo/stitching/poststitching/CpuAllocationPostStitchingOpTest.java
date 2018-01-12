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

    private final EntitySettingSpecs overprovisionSettingType = EntitySettingSpecs.CpuOverprovisionedPercentage;
    private final float overprovisionPercentage = 150;


    private final CommodityType sourceCommodityType = CommodityType.CPU;
    private final double sourceCapacity = 250;
    private final CommoditySoldDTO sourceCommodity = makeCommoditySold(sourceCommodityType, sourceCapacity);

    private final CommodityType computeCommodityType = CommodityType.CPU_ALLOCATION;
    private final CommoditySoldDTO originalComputeCommodity = makeCommoditySold(computeCommodityType);
    private final double expectedComputeCapacity = sourceCapacity * overprovisionPercentage / 100;


    private final List<CommoditySoldDTO> requiredCommodities = ImmutableList.of(originalComputeCommodity, sourceCommodity, irrelevantCommodity);
    private final List<CommoditySoldDTO> expectedCommodities = ImmutableList.of(sourceCommodity, irrelevantCommodity,
        makeCommoditySold(computeCommodityType, expectedComputeCapacity));




    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

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

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            Collections.emptyList());

    }

    @Test
    public void testNoSettings() {
        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
            eq(overprovisionSettingType))).thenReturn(Optional.empty());

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), requiredCommodities);
    }

    @Test
    public void testNoSourceCommodity() {

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(originalComputeCommodity, irrelevantCommodity);
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
    public void testNoComputeCommodity() {

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), origCommodities);
    }

    @Test
    public void testComputeCommodityWithCapacity() {

        final CommoditySoldDTO preloadedCompute = makeCommoditySold(computeCommodityType, 99);
        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(sourceCommodity, irrelevantCommodity, preloadedCompute);
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

        final CommoditySoldDTO computeWithKey = makeCommoditySold(computeCommodityType, key1);
        final CommoditySoldDTO sourceWithKey = makeCommoditySold(sourceCommodityType, sourceCapacity, key2);

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(sourceWithKey, irrelevantCommodity, computeWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), origCommodities);
    }

    @Test
    public void testOneHasKeyOtherDoesnt() {

        final String key = "abc";

        final CommoditySoldDTO computeWithKey = makeCommoditySold(computeCommodityType, key);

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(sourceCommodity, irrelevantCommodity, computeWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), origCommodities);
    }

    @Test
    public void testDuplicateCommodities() {

        final List<CommoditySoldDTO> origCommodities = new ArrayList<>();
        final CommoditySoldDTO duplicateCommodity = makeCommoditySold(sourceCommodityType, sourceCapacity);
        origCommodities.addAll(requiredCommodities);
        origCommodities.add(duplicateCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        try {
            result.getChanges().forEach(TopologicalChange::applyChange);
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
        final CommoditySoldDTO sourceCommodityWithKey =
            makeCommoditySold(sourceCommodityType, sourceCapacity, key);

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(computeCommodityType, key), sourceCommodityWithKey);

        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        final List<CommoditySoldDTO> actualCommodities =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        final List<CommoditySoldDTO> expectedCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(computeCommodityType, expectedComputeCapacity, key),
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
            makeTopologyEntity(Arrays.asList(secondSourceCommodity, originalComputeCommodity));

        final List<CommoditySoldDTO> thirdCommodityList =
            Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity thirdTestTE = makeTopologyEntity(thirdCommodityList);

        final CommoditySoldDTO commodityWithCapacity = makeCommoditySold(computeCommodityType, 4);
        final List<CommoditySoldDTO> fourthCommodityList =
            Arrays.asList(sourceCommodity, commodityWithCapacity, irrelevantCommodity);
        final TopologyEntity fourthTestTE = makeTopologyEntity(fourthCommodityList);

        final TopologicalChangelog result = operation.performOperation(Stream.of(testTE,
            secondTestTE, thirdTestTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        final List<CommoditySoldDTO> firstResult =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        assertTrue(firstResult.containsAll(expectedCommodities));
        assertTrue(expectedCommodities.containsAll(firstResult));

        final List<CommoditySoldDTO> secondExpectedResult = Arrays.asList(secondSourceCommodity,
            makeCommoditySold(computeCommodityType, secondCapacity * overprovisionPercentage / 100));
        assertEquals(secondTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            secondExpectedResult);

        assertEquals(thirdTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            thirdCommodityList);
        assertEquals(fourthTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            fourthCommodityList);
    }
}


