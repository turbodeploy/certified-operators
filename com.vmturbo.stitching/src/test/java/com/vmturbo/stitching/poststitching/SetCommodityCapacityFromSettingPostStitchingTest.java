package com.vmturbo.stitching.poststitching;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit test for {@link SetCommodityCapacityFromSettingPostStitchingOperation}.
 */
public class SetCommodityCapacityFromSettingPostStitchingTest {

    private static final CommodityType COMM_TYPE = CommodityType.RESPONSE_TIME;
    private static final CommodityType COMM_TYPE_EXCLUDED = CommodityType.DB_MEM;
    private static final String RESPONSE_TIME_SLO_SETTING = "responseTimeSLO";

    private static final EntityType SELLER_TYPE = EntityType.DATABASE_SERVER;

    private final SetCommodityCapacityFromSettingPostStitchingOperation stitchOperation =
            new SetCommodityCapacityFromSettingPostStitchingOperation(SELLER_TYPE,
                    ProbeCategory.DATABASE_SERVER, COMM_TYPE, RESPONSE_TIME_SLO_SETTING);

    private static final double initialCapacity = 10.0d;

    private static final float settingCapacityValue = 1000.0f;

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    private final EntitySettingsCollection entitySettingsCollection =
            mock(EntitySettingsCollection.class);


    /**
     * Test that the response time commodity capacity gets set to the setting value.
     */
    @Test
    public void testSetCapacityValue() {
        TopologyEntity provider = seller();
        Setting responseTimeSLOSetting = Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue
                        .newBuilder().setValue(settingCapacityValue).build()).build();
        when(entitySettingsCollection.getEntitySetting(provider.getOid(),
                RESPONSE_TIME_SLO_SETTING))
                .thenReturn(Optional.of(responseTimeSLOSetting));

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), entitySettingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        // check that COMM_TYPE commodity had it's capacity set to the value of the setting
        Assert.assertEquals(settingCapacityValue,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(),
                1e-5);
        // check that COMM_TYPE_EXCLUDED doesn't get its capacity changed
        Assert.assertEquals(initialCapacity,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(1).getCapacity(),
                1e-5);
    }

    @Test
    public void testSettingDoesNotExist() {
        TopologyEntity provider = seller();
        // make sure operation fails to get a setting for this provider during post stitching
        when(entitySettingsCollection.getEntitySetting(provider.getOid(),
                RESPONSE_TIME_SLO_SETTING))
                .thenReturn(Optional.empty());

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), entitySettingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        // check that COMM_TYPE commodity capacity was not changed by the poststitching operation
        Assert.assertEquals(initialCapacity,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getCapacity(),
                1e-5);
    }

    /**
     * Create a seller selling two commodities.
     * @return a seller that sells one commodity that is subject to the post stitching operations
     * and one that is not.
     */
    private TopologyEntity seller() {
        final long sellerOid = 111L;
        final TopologyEntity.Builder seller =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        sellerOid,
                        SELLER_TYPE.getNumber(),
                        Lists.newArrayList(
                                PostStitchingTestUtilities.makeCommoditySold(COMM_TYPE),
                                PostStitchingTestUtilities.makeCommoditySold(COMM_TYPE_EXCLUDED)),
                        Collections.emptyList());
        seller.getEntityBuilder().getCommoditySoldListBuilderList().forEach(builder ->
                builder.setCapacity(initialCapacity));
        return seller.build();
    }
}
