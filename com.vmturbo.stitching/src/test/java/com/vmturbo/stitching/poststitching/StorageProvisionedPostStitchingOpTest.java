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
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;
import com.vmturbo.stitching.poststitching.StorageProvisionedPostStitchingOperation.StorageEntityStorageProvisionedPostStitchingOperation;

@RunWith(Parameterized.class)
public class StorageProvisionedPostStitchingOpTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {new StorageProvisionedPostStitchingOperation.DiskArrayStorageProvisionedPostStitchingOperation()},
            {new StorageEntityStorageProvisionedPostStitchingOperation()},
            {new StorageProvisionedPostStitchingOperation.LogicalPoolStorageProvisionedPostStitchingOperation()}
        });
    }

    public StorageProvisionedPostStitchingOpTest(
            @Nonnull final OverprovisionCapacityPostStitchingOperation op) {
        this.operation = op;
    }

    private final OverprovisionCapacityPostStitchingOperation operation;
    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final CommoditySoldDTO irrelevantCommodity = makeCommoditySold(CommodityType.BALLOONING);

    private final float overprovisionPercentage = 150;


    private final double amountCapacity = 250;
    private final CommoditySoldDTO amountCommodity =
        makeCommoditySold(CommodityType.STORAGE_AMOUNT, amountCapacity);

    private final CommoditySoldDTO emptyProvisionedCommodity =
            makeCommoditySold(CommodityType.STORAGE_PROVISIONED);
    private final double expectedProvisionedCapacity =
            amountCapacity * overprovisionPercentage / 100;


    private final List<CommoditySoldDTO> requiredCommodities =
            ImmutableList.of(emptyProvisionedCommodity, amountCommodity, irrelevantCommodity);
    private final List<CommoditySoldDTO> expectedCommodities =
        ImmutableList.of(amountCommodity, irrelevantCommodity,
            makeCommoditySold(CommodityType.STORAGE_PROVISIONED, expectedProvisionedCapacity));

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();

        final Setting overprovisionSetting = makeNumericSetting(overprovisionPercentage);
        when(settingsMock.getEntitySetting(any(TopologyEntity.class),
            eq(EntitySettingSpecs.StorageOverprovisionedPercentage)))
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
                eq(EntitySettingSpecs.StorageOverprovisionedPercentage))).thenReturn(Optional.empty());

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoAmountCommodity() {

        final List<CommoditySoldDTO> origCommodities =
                Arrays.asList(emptyProvisionedCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoProvisionedCommodity() {

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(amountCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testProvisionedCommodityWithCapacity() {

        final CommoditySoldDTO preloadedProvisioned = makeCommoditySold(CommodityType.STORAGE_PROVISIONED, 99);
        final List<CommoditySoldDTO> origCommodities =
                Arrays.asList(amountCommodity, irrelevantCommodity, preloadedProvisioned);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testMismatchingKeys() {

        final String key1 = "abc";
        final String key2 = "xyz";

        final CommoditySoldDTO provisionedWithKey = makeCommoditySold(CommodityType.STORAGE_PROVISIONED, key1);
        final CommoditySoldDTO sourceWithKey = makeCommoditySold(CommodityType.STORAGE_AMOUNT, amountCapacity, key2);

        final List<CommoditySoldDTO> origCommodities =
                Arrays.asList(sourceWithKey, irrelevantCommodity, provisionedWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testOneHasKeyOtherDoesnt() {

        final String key = "abc";

        final CommoditySoldDTO provisionedWithKey = makeCommoditySold(CommodityType.STORAGE_PROVISIONED, key);

        final List<CommoditySoldDTO> origCommodities =
                Arrays.asList(amountCommodity, irrelevantCommodity, provisionedWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testDuplicateCommodities() {

        final List<CommoditySoldDTO> origCommodities = new ArrayList<>();
        final CommoditySoldDTO duplicateCommodity = makeCommoditySold(CommodityType.STORAGE_AMOUNT, amountCapacity);
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
                    CommodityType.STORAGE_AMOUNT +" with key "+ duplicateCommodity.getCommodityType().getKey() +
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
                makeCommoditySold(CommodityType.STORAGE_AMOUNT, amountCapacity, key);

        final List<CommoditySoldDTO> origCommodities = Arrays.asList(irrelevantCommodity,
                makeCommoditySold(CommodityType.STORAGE_PROVISIONED, key), sourceCommodityWithKey);

        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        final List<CommoditySoldDTO> actualCommodities =
                testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        final List<CommoditySoldDTO> expectedCommodities = Arrays.asList(irrelevantCommodity,
                makeCommoditySold(CommodityType.STORAGE_PROVISIONED, expectedProvisionedCapacity, key),
                sourceCommodityWithKey);

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }

    @Test
    public void testMultipleEntities() {

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        final double secondCapacity = 500;
        final CommoditySoldDTO secondSourceCommodity = makeCommoditySold(CommodityType.STORAGE_AMOUNT, secondCapacity);
        final TopologyEntity secondTestTE =
                makeTopologyEntity(Arrays.asList(secondSourceCommodity, emptyProvisionedCommodity));

        final List<CommoditySoldDTO> thirdCommodityList =
                Arrays.asList(amountCommodity, irrelevantCommodity);
        final TopologyEntity thirdTestTE = makeTopologyEntity(thirdCommodityList);

        final CommoditySoldDTO commodityWithCapacity = makeCommoditySold(CommodityType.STORAGE_PROVISIONED, 4);
        final List<CommoditySoldDTO> fourthCommodityList =
                Arrays.asList(amountCommodity, commodityWithCapacity, irrelevantCommodity);
        final TopologyEntity fourthTestTE = makeTopologyEntity(fourthCommodityList);

        final TopologicalChangelog result = operation.performOperation(Stream.of(testTE,
                secondTestTE, thirdTestTE), settingsMock, resultBuilder);
        result.getChanges().forEach(TopologicalChange::applyChange);

        final List<CommoditySoldDTO> firstResult =
                testTE.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        assertTrue(firstResult.containsAll(expectedCommodities));
        assertTrue(expectedCommodities.containsAll(firstResult));

        final List<CommoditySoldDTO> secondExpectedResult = Arrays.asList(secondSourceCommodity,
                makeCommoditySold(CommodityType.STORAGE_PROVISIONED, secondCapacity * overprovisionPercentage / 100));
        assertEquals(secondTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
                secondExpectedResult);

        assertEquals(thirdTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
                thirdCommodityList);
        assertEquals(fourthTestTE.getTopologyEntityDtoBuilder().getCommoditySoldListList(),
                fourthCommodityList);
    }
}


