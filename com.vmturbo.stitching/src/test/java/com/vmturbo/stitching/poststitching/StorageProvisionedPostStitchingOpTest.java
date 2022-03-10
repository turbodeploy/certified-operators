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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
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

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    public StorageProvisionedPostStitchingOpTest(
            @Nonnull final OverprovisionCapacityPostStitchingOperation op) {
        this.operation = op;
    }

    private final OverprovisionCapacityPostStitchingOperation operation;
    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final CommoditySoldView irrelevantCommodity = makeCommoditySold(CommodityType.BALLOONING);

    private final float overprovisionPercentage = 150;


    private final double amountCapacity = 250;
    private final CommoditySoldView amountCommodity =
        makeCommoditySold(CommodityType.STORAGE_AMOUNT, amountCapacity);

    private final CommoditySoldView emptyProvisionedCommodity =
            makeCommoditySold(CommodityType.STORAGE_PROVISIONED);
    private final double expectedProvisionedCapacity =
            amountCapacity * overprovisionPercentage / 100;


    private final List<CommoditySoldView> requiredCommodities =
            ImmutableList.of(emptyProvisionedCommodity, amountCommodity, irrelevantCommodity);
    private final List<CommoditySoldView> expectedCommodities =
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
        assertTrue(resultBuilder.getChanges().size() == 1);
    }

    @Test
    public void testNoAmountCommodity() {

        final List<CommoditySoldView> origCommodities =
                Arrays.asList(emptyProvisionedCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    @Test
    public void testNoProvisionedCommodity() {

        final List<CommoditySoldView> origCommodities = Arrays.asList(amountCommodity, irrelevantCommodity);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testProvisionedCommodityWithCapacity() {

        final CommoditySoldView preloadedProvisioned = makeCommoditySold(CommodityType.STORAGE_PROVISIONED, 99);
        final List<CommoditySoldView> origCommodities =
                Arrays.asList(amountCommodity, irrelevantCommodity, preloadedProvisioned);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testMismatchingKeys() {

        final String key1 = "abc";
        final String key2 = "xyz";

        final CommoditySoldView provisionedWithKey = makeCommoditySold(CommodityType.STORAGE_PROVISIONED, key1);
        final CommoditySoldView sourceWithKey = makeCommoditySold(CommodityType.STORAGE_AMOUNT, amountCapacity, key2);

        final List<CommoditySoldView> origCommodities =
                Arrays.asList(sourceWithKey, irrelevantCommodity, provisionedWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testOneHasKeyOtherDoesnt() {

        final String key = "abc";

        final CommoditySoldView provisionedWithKey = makeCommoditySold(CommodityType.STORAGE_PROVISIONED, key);

        final List<CommoditySoldView> origCommodities =
                Arrays.asList(amountCommodity, irrelevantCommodity, provisionedWithKey);
        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());

    }

    @Test
    public void testDuplicateCommodities() {

        final List<CommoditySoldView> origCommodities = new ArrayList<>();
        final CommoditySoldView duplicateCommodity = makeCommoditySold(CommodityType.STORAGE_AMOUNT, amountCapacity);
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
                    CommodityType.STORAGE_AMOUNT +" with key "+ duplicateCommodity.getCommodityType().getKey() +
                    " in entity 0");
        }
    }

    @Test
    public void testHappyPathNoKeys() {

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

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
                makeCommoditySold(CommodityType.STORAGE_AMOUNT, amountCapacity, key);

        final List<CommoditySoldView> origCommodities = Arrays.asList(irrelevantCommodity,
                makeCommoditySold(CommodityType.STORAGE_PROVISIONED, key), sourceCommodityWithKey);

        final TopologyEntity testTE = makeTopologyEntity(origCommodities);

        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldView> actualCommodities =
                testTE.getTopologyEntityImpl().getCommoditySoldListList();
        final List<CommoditySoldView> expectedCommodities = Arrays.asList(irrelevantCommodity,
                makeCommoditySold(CommodityType.STORAGE_PROVISIONED, expectedProvisionedCapacity, key),
                sourceCommodityWithKey);

        assertTrue(expectedCommodities.containsAll(actualCommodities));
        assertTrue(actualCommodities.containsAll(expectedCommodities));
    }

    @Test
    public void testMultipleEntities() {

        final TopologyEntity testTE = makeTopologyEntity(requiredCommodities);

        final double secondCapacity = 500;
        final CommoditySoldView secondSourceCommodity = makeCommoditySold(CommodityType.STORAGE_AMOUNT, secondCapacity);
        final TopologyEntity secondTestTE =
                makeTopologyEntity(Arrays.asList(secondSourceCommodity, emptyProvisionedCommodity));

        final List<CommoditySoldView> thirdCommodityList =
                Arrays.asList(amountCommodity, irrelevantCommodity);
        final TopologyEntity thirdTestTE = makeTopologyEntity(thirdCommodityList);

        final CommoditySoldView commodityWithCapacity = makeCommoditySold(CommodityType.STORAGE_PROVISIONED, 4);
        final List<CommoditySoldView> fourthCommodityList =
                Arrays.asList(amountCommodity, commodityWithCapacity, irrelevantCommodity);
        final TopologyEntity fourthTestTE = makeTopologyEntity(fourthCommodityList);

        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(Stream.of(testTE,
                secondTestTE, thirdTestTE), settingsMock, resultBuilder);
        result.getChanges().forEach(change -> change.applyChange(journal));

        final List<CommoditySoldView> firstResult =
                testTE.getTopologyEntityImpl().getCommoditySoldListList();
        assertTrue(firstResult.containsAll(expectedCommodities));
        assertTrue(expectedCommodities.containsAll(firstResult));

        final List<CommoditySoldView> secondExpectedResult = Arrays.asList(secondSourceCommodity,
                makeCommoditySold(CommodityType.STORAGE_PROVISIONED, secondCapacity * overprovisionPercentage / 100));
        assertEquals(secondTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
                secondExpectedResult);

        assertEquals(thirdTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
                thirdCommodityList);
        assertEquals(fourthTestTE.getTopologyEntityImpl().getCommoditySoldListList(),
                fourthCommodityList);
    }

    @Test
    public void testSetStorageProvisionedBoughtUsed() {
        // mock sold storage amount whose capacity value comes from vc
        CommoditySoldView soldStorageAmount = makeCommoditySold(CommodityType.STORAGE_AMOUNT, 1200);
        CommoditySoldView soldStorageLatency = makeCommoditySold(CommodityType.STORAGE_LATENCY, 5000);
        CommoditySoldView soldStorageProvisioned = makeCommoditySold(CommodityType.STORAGE_PROVISIONED);

        // mock bought storage provisioned whose used value comes from storage probe
        CommodityBoughtView boughtStorageProvisioned = new CommodityBoughtImpl()
                .setCommodityType(new CommodityTypeImpl()
                        .setType(CommodityType.STORAGE_PROVISIONED_VALUE))
                .setUsed(1500);
        CommodityBoughtView boughtStorageLatency = new CommodityBoughtImpl()
                .setCommodityType(new CommodityTypeImpl()
                        .setType(CommodityType.STORAGE_LATENCY_VALUE))
                .setUsed(2);

        final TopologyEntity storage = makeTopologyEntity(EntityType.STORAGE_VALUE,
                Lists.newArrayList(soldStorageAmount, soldStorageLatency, soldStorageProvisioned),
                Lists.newArrayList(boughtStorageLatency, boughtStorageProvisioned),
                Collections.emptyList());

        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(
                Stream.of(storage), settingsMock, resultBuilder);

        if (operation instanceof StorageEntityStorageProvisionedPostStitchingOperation) {
            // check that the number of changes is 2 (one is for StorageProvisioned sold capacity,
            // the other is for StorageProvisioned bought used
            assertEquals(2, result.getChanges().size());
            result.getChanges().forEach(change -> change.applyChange(journal));

            // check that StorageProvisioned bought used value is set to StorageAmount sold capacity
            assertEquals(1200, storage.getTopologyEntityImpl()
                    .getCommoditiesBoughtFromProviders(0).getCommodityBoughtList().stream()
                    .filter(commodityBoughtDTO -> commodityBoughtDTO.getCommodityType().getType() ==
                            CommodityType.STORAGE_PROVISIONED_VALUE)
                    .map(CommodityBoughtView::getUsed).findAny().get(), 0);
        } else {
            // only one change for StorageProvisioned sold capacity
            assertEquals(1, result.getChanges().size());
        }
    }

    @Test
    public void testSetStorageProvisionedBoughtUsed_NoStorageProvisonedBought() {
        // mock sold storage amount whose capacity value comes from vc
        CommoditySoldView soldStorageAmount = makeCommoditySold(CommodityType.STORAGE_AMOUNT, 1200);
        CommoditySoldView soldStorageLatency = makeCommoditySold(CommodityType.STORAGE_LATENCY, 5000);
        CommoditySoldView soldStorageProvisioned = makeCommoditySold(CommodityType.STORAGE_PROVISIONED);

        final TopologyEntity storage = makeTopologyEntity(EntityType.STORAGE_VALUE,
                Lists.newArrayList(soldStorageAmount, soldStorageLatency, soldStorageProvisioned));
        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(
                Stream.of(storage), settingsMock, resultBuilder);
        // only one change for StorageProvisioned sold capacity
        assertEquals(1, result.getChanges().size());
    }

    @Test
    public void testSetStorageProvisionedBoughtUsed_NoStorageAmountSold() {
        // mock sold commodities
        CommoditySoldView soldStorageLatency = makeCommoditySold(CommodityType.STORAGE_LATENCY, 5000);
        CommoditySoldView soldStorageProvisioned = makeCommoditySold(CommodityType.STORAGE_PROVISIONED);
        // mock bought commodities
        CommodityBoughtView boughtStorageProvisioned = new CommodityBoughtImpl()
                .setCommodityType(new CommodityTypeImpl()
                        .setType(CommodityType.STORAGE_PROVISIONED_VALUE))
                .setUsed(1500);
        CommodityBoughtView boughtStorageLatency = new CommodityBoughtImpl()
                .setCommodityType(new CommodityTypeImpl()
                        .setType(CommodityType.STORAGE_LATENCY_VALUE))
                .setUsed(2);

        final TopologyEntity storage = makeTopologyEntity(EntityType.STORAGE_VALUE,
                Lists.newArrayList(soldStorageLatency, soldStorageProvisioned),
                Lists.newArrayList(boughtStorageLatency, boughtStorageProvisioned),
                Collections.emptyList());

        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(
                Stream.of(storage), settingsMock, resultBuilder);
        // no change for StorageProvisioned sold capacity, or StorageProvisioned bought used
        // since StorageAmount is not sold
        assertEquals(0, result.getChanges().size());
    }

    /**
     * Test that storage provisioned bought used value is not being updated for virtual volumes,
     * we should be using value passed from the probe.
     */
    @Test
    public void testSetVirtualVolumeStorageProvisionedBoughtUsed() {

        final CommoditySoldView soldStorageAmount = makeCommoditySold(CommodityType.STORAGE_AMOUNT, 3000);

        final CommodityBoughtView boughtStorageProvisioned = new CommodityBoughtImpl()
            .setCommodityType(new CommodityTypeImpl()
                .setType(CommodityType.STORAGE_PROVISIONED_VALUE))
            .setUsed(1500);

        final TopologyEntity virtualVolume = makeTopologyEntity(EntityType.VIRTUAL_VOLUME_VALUE,
            Lists.newArrayList(soldStorageAmount),
            Lists.newArrayList(boughtStorageProvisioned),
            Collections.emptyList());

        final TopologicalChangelog<TopologyEntity> result = operation.performOperation(
            Stream.of(virtualVolume), settingsMock, resultBuilder);
        if (operation instanceof StorageEntityStorageProvisionedPostStitchingOperation) {
            // For virtual volumes we do  not expect storage provisioned bought used value to be
            // updated by post-stitching operation
            assertEquals(0, result.getChanges().size());
        }
    }
}


