package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeNumericSetting;
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

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.journal.IStitchingJournal;
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

    private final CommoditySoldView sourceCommodity;
    private final CommodityType sourceCommodityType;

    private final CommodityType overprovisionedCommodityType;
    private final CommoditySoldView originalOverprovisionedCommodity;

    private final List<CommoditySoldView> requiredCommodities;
    private final List<CommoditySoldView> expectedCommodities;

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

    private final CommoditySoldView irrelevantCommodity = makeCommoditySold(CommodityType.BALLOONING);

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
            Arrays.asList(originalOverprovisionedCommodity, irrelevantCommodity);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoOverprovisionedCommodity() {

        final List<CommoditySoldView> origCommodities = Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testOverprovisionedCommodityWithCapacity() {

        final CommoditySoldView preloadedOverprovisioned = makeCommoditySold(overprovisionedCommodityType, 99);
        final List<CommoditySoldView> origCommodities =
            Arrays.asList(sourceCommodity, irrelevantCommodity, preloadedOverprovisioned);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        assertEquals(testTE.getTopologyEntityImpl().getCommoditySoldListList(), expectedCommodities);
    }

    @Test
    public void testMismatchingKeys() {

        final String key1 = "abc";
        final String key2 = "xyz";

        final CommoditySoldView overprovisionedWithKey = makeCommoditySold(overprovisionedCommodityType, key1);
        final CommoditySoldView sourceWithKey = makeCommoditySold(sourceCommodityType, sourceCapacity, key2);

        final List<CommoditySoldView> origCommodities =
            Arrays.asList(sourceWithKey, irrelevantCommodity, overprovisionedWithKey);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testOneHasKeyOtherDoesnt() {

        final String key = "abc";

        final CommoditySoldView overprovisionedWithKey = makeCommoditySold(overprovisionedCommodityType, key);

        final List<CommoditySoldView> origCommodities =
            Arrays.asList(sourceCommodity, irrelevantCommodity, overprovisionedWithKey);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testDuplicateCommodities() {

        final List<CommoditySoldView> origCommodities = new ArrayList<>();
        final CommoditySoldView duplicateCommodity = makeCommoditySold(sourceCommodityType, sourceCapacity);
        origCommodities.addAll(requiredCommodities);
        origCommodities.add(duplicateCommodity);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);
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

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(requiredCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

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
            makeCommoditySold(overprovisionedCommodityType, key), sourceCommodityWithKey);

        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(origCommodities);

        final TopologicalChangelog<TopologyEntity> result =
            operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldView> actualCommodities =
            testTE.getTopologyEntityImpl().getCommoditySoldListList();
        final List<CommoditySoldView> expectedCommodities = Arrays.asList(irrelevantCommodity,
            makeCommoditySold(overprovisionedCommodityType, expectedOverprovisionedCapacity, key),
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
            PostStitchingTestUtilities.makeTopologyEntity(Arrays.asList(secondSourceCommodity, originalOverprovisionedCommodity));

        final List<CommoditySoldView> thirdCommodityList =
            Arrays.asList(sourceCommodity, irrelevantCommodity);
        final TopologyEntity thirdTestTE = PostStitchingTestUtilities.makeTopologyEntity(thirdCommodityList);

        final CommoditySoldView commodityWithCapacity = makeCommoditySold(overprovisionedCommodityType, 4);
        final List<CommoditySoldView> fourthCommodityList =
            Arrays.asList(sourceCommodity, commodityWithCapacity, irrelevantCommodity);
        final TopologyEntity fourthTestTE = PostStitchingTestUtilities.makeTopologyEntity(fourthCommodityList);

        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(Stream.of(testTE,
            secondTestTE, thirdTestTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldView> firstResult =
            testTE.getTopologyEntityImpl().getCommoditySoldListList();
        assertTrue(firstResult.containsAll(expectedCommodities));
        assertTrue(expectedCommodities.containsAll(firstResult));

        final List<CommoditySoldView> secondExpectedResult = Arrays.asList(secondSourceCommodity,
            makeCommoditySold(overprovisionedCommodityType, secondCapacity * overprovisionPercentage / 100));
        assertEquals(secondTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
            secondExpectedResult);

        assertEquals(thirdTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
            thirdCommodityList);
        assertEquals(fourthTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
            fourthCommodityList);
    }


}
