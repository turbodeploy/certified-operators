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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.CpuProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.MemoryProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

@RunWith(Parameterized.class)
public class OverprovisionedCommodityPostStitchingOpTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {new CpuProvisionedPostStitchingOperation(), CommodityType.CPU,
                CommodityType.CPU_PROVISIONED, EntitySettingSpecs.CpuOverprovisionedPercentage},
            {new MemoryProvisionedPostStitchingOperation(), CommodityType.MEM,
                CommodityType.MEM_PROVISIONED, EntitySettingSpecs.MemoryOverprovisionedPercentage}
        });
    }

    private final OverprovisionCapacityPostStitchingOperation operation;

    private final EntitySettingSpecs overprovisionSettingType;

    private final CommoditySoldDTO sourceCommodity;
    private final CommodityType sourceCommodityType;

    private final CommodityType overprovisionedCommodityType;
    private final CommoditySoldDTO originalOverprovisionedCommodity;

    private final List<CommoditySoldDTO> requiredCommodities;
    private final List<CommoditySoldDTO> expectedCommodities;

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    public OverprovisionedCommodityPostStitchingOpTest(
                                @Nonnull final OverprovisionCapacityPostStitchingOperation op,
                                @Nonnull final CommodityType sourceType,
                                @Nonnull final CommodityType overprovisionedType,
                                @Nonnull final EntitySettingSpecs settingType) {
        this.operation = op;
        this.sourceCommodityType = sourceType;
        this.sourceCommodity = makeCommoditySold(sourceCommodityType, sourceCapacity);
        this.overprovisionedCommodityType = overprovisionedType;
        this.originalOverprovisionedCommodity = makeCommoditySold(overprovisionedCommodityType);

        this.overprovisionSettingType = settingType;

        this.requiredCommodities =
            ImmutableList.of(originalOverprovisionedCommodity, sourceCommodity, irrelevantCommodity);
        this.expectedCommodities = ImmutableList.of(sourceCommodity, irrelevantCommodity,
            makeCommoditySold(overprovisionedCommodityType, expectedOverprovisionedCapacity));
    }

    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final double sourceCapacity = 250;
    private final float overprovisionPercentage = 150;
    private final double expectedOverprovisionedCapacity = sourceCapacity * overprovisionPercentage / 100;

    private final CommoditySoldDTO irrelevantCommodity = makeCommoditySold(CommodityType.BALLOONING);

    private final Setting overprovisionSetting = makeNumericSetting(overprovisionPercentage);

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();

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
            Arrays.asList(originalOverprovisionedCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoOverprovisionedCommodity() {

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testOverprovisionedCommodityWithCapacity() {

        final CommoditySoldDTO preloadedOverprovisioned = makeCommoditySold(overprovisionedCommodityType, 99);
        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(sourceCommodity, irrelevantCommodity, preloadedOverprovisioned);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertEquals(testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(), expectedCommodities);
    }

    @Test
    public void testMismatchingKeys() {

        final String key1 = "abc";
        final String key2 = "xyz";

        final CommoditySoldDTO overprovisionedWithKey = makeCommoditySold(overprovisionedCommodityType, key1);
        final CommoditySoldDTO sourceWithKey = makeCommoditySold(sourceCommodityType, sourceCapacity, key2);

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(sourceWithKey, irrelevantCommodity, overprovisionedWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testOneHasKeyOtherDoesnt() {

        final String key = "abc";

        final CommoditySoldDTO overprovisionedWithKey = makeCommoditySold(overprovisionedCommodityType, key);

        final List<CommoditySoldDTO> origCommodities =
            Arrays.asList(sourceCommodity, irrelevantCommodity, overprovisionedWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testDuplicateCommodities() {

        final List<CommoditySoldDTO> origCommodities = new ArrayList<>();
        final CommoditySoldDTO duplicateCommodity = makeCommoditySold(sourceCommodityType, sourceCapacity);
        origCommodities.addAll(requiredCommodities);
        origCommodities.add(duplicateCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);
        try {
            final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
            result.getChanges().forEach(change -> change.applyChange(journal));
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
        result.getChanges().forEach(change -> change.applyChange(journal));

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
            makeCommoditySold(overprovisionedCommodityType, key), sourceCommodityWithKey);

        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldDTO> actualCommodities =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        final List<CommoditySoldDTO> expectedCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(overprovisionedCommodityType, expectedOverprovisionedCapacity, key),
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
            makeTopologyEntity(Arrays.asList(secondSourceCommodity, originalOverprovisionedCommodity));

        final List<CommoditySoldDTO> thirdCommodityList =
            Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity thirdTestTE = makeTopologyEntity(thirdCommodityList);

        final CommoditySoldDTO commodityWithCapacity = makeCommoditySold(overprovisionedCommodityType, 4);
        final List<CommoditySoldDTO> fourthCommodityList =
            Arrays.asList(sourceCommodity, commodityWithCapacity, irrelevantCommodity);
        final TopologyEntity fourthTestTE = makeTopologyEntity(fourthCommodityList);

        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(Stream.of(testTE,
            secondTestTE, thirdTestTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldDTO> firstResult =
            testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        assertTrue(firstResult.containsAll(expectedCommodities));
        assertTrue(expectedCommodities.containsAll(firstResult));

        final List<CommoditySoldDTO> secondExpectedResult = Arrays.asList(secondSourceCommodity,
            makeCommoditySold(overprovisionedCommodityType, secondCapacity * overprovisionPercentage / 100));
        assertEquals(secondTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            secondExpectedResult);

        assertEquals(thirdTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            thirdCommodityList);
        assertEquals(fourthTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
            fourthCommodityList);
    }


}
