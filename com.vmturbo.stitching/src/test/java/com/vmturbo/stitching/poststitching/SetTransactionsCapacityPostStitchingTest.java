package com.vmturbo.stitching.poststitching;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;
import com.vmturbo.stitching.poststitching.SetAutoSetCommodityCapacityPostStitchingOperation.MaxCapacityCache;

/**
 * Unit test for {@link SetTransactionsCapacityPostStitchingOperation}.
 */
public class SetTransactionsCapacityPostStitchingTest {
    private static final String SLO_SETTING = "transactionSLO";
    private static final String SLO_ENABLED_SETTING = "transactionSLOEnabled";
    /**
     * The entity type of the consumer.
     */
    private static final EntityType BUYER_TYPE = EntityType.SERVICE;
    private static final EntityType SELLER_TYPE = EntityType.DATABASE_SERVER;

    private static SetTransactionsCapacityPostStitchingOperation stitchOperation;

    private static final double initialCapacity = 10.0d;

    private static final float settingCapacityValue = 1000.0f;

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    private final EntitySettingsCollection entitySettingsCollection =
            mock(EntitySettingsCollection.class);

    private final Setting sloSettingTrue = Setting.newBuilder()
            .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true).build())
            .build();

    private final Setting sloSettingFalse = Setting.newBuilder()
            .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(false).build())
            .build();

    private final MaxCapacityCache maxCapacityCache = mock(MaxCapacityCache.class);

    /**
     * Common code before every test.
     */
    @Before
    public void init() {
        stitchOperation = new SetTransactionsCapacityPostStitchingOperation(SELLER_TYPE,
            ProbeCategory.DATABASE_SERVER, SLO_SETTING,
            (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
            SLO_ENABLED_SETTING, maxCapacityCache);
    }

    /**
     * Test that the transaction capacity is set to the setting value when autoset is true and
     * the setting value is larger than the used or capacity value of the commodity.
     */
    @Test
    public void testSettingCapacityIsMax() {
        // create a seller with no used value and capacity < setting value.  Expect setting value
        // to replace capacity value
        TopologyEntity provider = seller(1L, Optional.empty(),
                Optional.of(initialCapacity));
        testSetCapacityValue(provider, true, settingCapacityValue, initialCapacity);
        // autoset is true but no value from db and the used value is 0 so expecting the policy capacity
        testSetCapacityValue(provider, false, settingCapacityValue, initialCapacity);
    }

    /**
     * Test the case where the initial capacity of the transaction commodity is larger than the
     * setting capacity value and autoset is true.  We expect the transaction capacity to remain
     * as it was before the operation.
     */
    @Test
    public void testCommodityCapacityIsMax() {
        // create a seller with no used value and capacity < setting value.  Expect setting value
        // to replace capacity value
        TopologyEntity provider = seller(1L, Optional.of(initialCapacity),
                Optional.of(settingCapacityValue * 2d));
        testSetCapacityValue(provider, true, settingCapacityValue * 2d,
                settingCapacityValue * 2d);
    }

    /**
     * Test the case where the intial capacity is higher than the setting capacity, but autoset
     * is false.  In this case, we expect the commodity capacity to be overwritten with the
     * setting capacity.
     */
    @Test
    public void testCommodityCapacityIsMaxAutoSetFalse() {
        // create a seller with no used value and capacity < setting value.  Expect setting value
        // to replace capacity value
        TopologyEntity provider = seller(1L, Optional.of(initialCapacity),
                Optional.of(settingCapacityValue * 2d));
        testSetCapacityValue(provider, true, settingCapacityValue * 2d,
                settingCapacityValue * 2d);
    }

    /**
     * Test the case where the transaction commodity used value is larger than the setting
     * value and the initial capacity of the transaction commodity.  We expect the used value to
     * be used as the capacity after the operation.
     */
    @Test
    public void testCommodityUsedIsMax() {
        // create a seller with no used value and capacity < setting value.  Expect setting value
        // to replace capacity value
        TopologyEntity provider = seller(1L, Optional.of(settingCapacityValue * 2d),
                Optional.of(initialCapacity));
        testSetCapacityValue(provider, true, settingCapacityValue * 2d,
                initialCapacity);
    }

    /**
     * Test the case where the SLO is not enabled, there is used value but the capacity is not set,
     * so the capacity is automatically set as the used value, and the commodity is set to inactive.
     */
    @Test
    public void testAutosetCapacityWithUsedValue() {
        // SLO is disabled, no value from db, used value is > 0, capacity is empty
        // Expecting the used value as capacity
        TopologyEntity provider = sellerWithBuyer(2L, 1L, initialCapacity);
        testSetCapacityValue(provider, false, initialCapacity, 0d);
        List<CommoditySoldDTO> commoditySoldDTOS = provider.getTopologyEntityDtoBuilder().getCommoditySoldListList();
        List<CommodityBoughtDTO> commodityBoughtDTOS = provider.getConsumers().stream()
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .map(TopologyEntityDTO.Builder::getCommoditiesBoughtFromProvidersList)
                .flatMap(List::stream)
                .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        // Verify sold commodities
        Assert.assertFalse(commoditySoldDTOS.get(0).getActive());
        Assert.assertTrue(commoditySoldDTOS.get(1).getActive());
        // Verify bought commodities
        Assert.assertFalse(commodityBoughtDTOS.get(0).getActive());
        Assert.assertTrue(commodityBoughtDTOS.get(1).getActive());
    }

    /**
     * Check if poststitching operation works correctly.  Assertions should succeed if the capacity
     * is set correctly and fail if not.
     *
     * @param provider {@link TopologyEntity} with commodity values set according to the design
     *                                       of the test.
     * @param sloEnabledFlag boolean value indicating value of the Enable SLO setting.
     * @param maxValue double giving the maximum value among the commodity used value, capacity,
     *                 and the transaction setting capacity.
     * @param startingCapacity the initial capacity value for the commodities the provider is selling.
     */
    private void testSetCapacityValue(TopologyEntity provider, boolean sloEnabledFlag, double maxValue,
                                      double startingCapacity) {
       Setting transactionSLOSetting = Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue
                        .newBuilder().setValue(settingCapacityValue).build()).build();
        when(entitySettingsCollection.getEntitySetting(provider.getOid(), SLO_SETTING))
                .thenReturn(Optional.of(transactionSLOSetting));
        when(entitySettingsCollection.getEntitySetting(provider.getOid(), SLO_ENABLED_SETTING))
                .thenReturn(sloEnabledFlag ? Optional.of(sloSettingTrue)
                        : Optional.of(sloSettingFalse));

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), entitySettingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        // check that transaction commodity had it's capacity set to the value of the setting
        Assert.assertEquals(sloEnabledFlag ? settingCapacityValue : maxValue,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(),
                1e-5);
        // check that response time commodity doesn't get its capacity changed
        Assert.assertEquals(startingCapacity,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(1).getCapacity(),
                1e-5);
    }

    @Test
    public void testSettingDoesNotExist() {
        TopologyEntity provider = seller(1L, Optional.empty(),
                Optional.of(initialCapacity));
        // make sure operation fails to get a setting for this provider during post stitching
        when(entitySettingsCollection.getEntitySetting(provider.getOid(), SLO_SETTING))
                .thenReturn(Optional.empty());
        when(entitySettingsCollection.getEntitySetting(provider.getOid(), SLO_ENABLED_SETTING))
                .thenReturn(Optional.empty());

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), entitySettingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        // check that transaction commodity capacity was not changed by the poststitching operation
        Assert.assertEquals(initialCapacity,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(),
                1e-5);
    }

    /**
     * Create a topology entity with the given oid and selling a transaction commodity and a
     * response time commodity with used and capacity values as given.
     *
     * @param sellerOid the oid for the topology entity.
     * @param usedValue an optional used value to give each commodity.  If it is Optional.empty,
     *                  no used value is set.
     * @param capacityValue an optional capacity value to give each commodity.  If it is
     *                      Optional.empty, no capacity value is set.
     * @return {@link TopologyEntity} with the commodity values as defined.
     */
    private TopologyEntity seller(long sellerOid, Optional<Double> usedValue,
                                  Optional<Double> capacityValue) {
        final TopologyEntity.Builder seller =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        sellerOid,
                        SELLER_TYPE.getNumber(),
                        Lists.newArrayList(
                                PostStitchingTestUtilities
                                        .makeCommoditySold(CommodityType.TRANSACTION),
                                PostStitchingTestUtilities
                                        .makeCommoditySold(CommodityType.RESPONSE_TIME)),
                        Collections.emptyList());
        seller.getEntityBuilder().getCommoditySoldListBuilderList().forEach(builder -> {
            usedValue.ifPresent(used -> builder.setUsed(used));
            capacityValue.ifPresent(capacity -> builder.setCapacity(capacity));

        });
        return seller.build();
    }

    /**
     * Create a seller entity with a buyer entity and trade response time and transaction
     * commodities between the seller and the buyer.
     *
     * @param buyerOid the oid for the buyer
     * @param sellerOid the oid for the seller
     * @param usedValue the used value for the commodity
     * @return the seller entity
     */
    private TopologyEntity sellerWithBuyer(long buyerOid, long sellerOid, double usedValue) {
        final TopologyEntity.Builder buyer =  TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder()
                        .setOid(buyerOid)
                        .setEntityType(BUYER_TYPE.getNumber())
                        .addCommoditiesBoughtFromProviders(
                                PostStitchingTestUtilities.makeCommoditiesBoughtFromProvider(
                                        sellerOid,
                                        SELLER_TYPE.getNumber(),
                                        Lists.newArrayList(
                                                PostStitchingTestUtilities
                                                        .makeCommodityBought(CommodityType.TRANSACTION),
                                                PostStitchingTestUtilities
                                                        .makeCommodityBought(CommodityType.RESPONSE_TIME))
                                )));
        final TopologyEntity.Builder seller =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        sellerOid,
                        SELLER_TYPE.getNumber(),
                        Lists.newArrayList(
                                PostStitchingTestUtilities
                                        .makeCommoditySold(CommodityType.TRANSACTION),
                                PostStitchingTestUtilities
                                        .makeCommoditySold(CommodityType.RESPONSE_TIME)),
                        Collections.emptyList());
        seller.getEntityBuilder().getCommoditySoldListBuilderList()
                .forEach(builder -> builder.setUsed(usedValue));
        return seller.addConsumer(buyer).build();
    }
}
