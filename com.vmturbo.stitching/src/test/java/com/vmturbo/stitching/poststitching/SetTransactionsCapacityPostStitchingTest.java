package com.vmturbo.stitching.poststitching;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit test for {@link SetTransactionsCapacityPostStitchingOperation}.
 */
public class SetTransactionsCapacityPostStitchingTest {
    private static final String SLO_SETTING = "transactionSLO";
    private static final String SLO_ENABLED_SETTING = "transactionSLOEnabled";

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

    @BeforeClass
    public static void init() {
        com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig config =
            mock(com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig.class);
        stitchOperation = new SetTransactionsCapacityPostStitchingOperation(SELLER_TYPE,
            ProbeCategory.DATABASE_SERVER, SLO_SETTING,
            (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
            SLO_ENABLED_SETTING, config);
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
}
