package com.vmturbo.topology.processor.group.settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Tests for class VsanStorageApplicator.
 */
public class VsanStorageApplicatorTest {

    private static final String RAID0 = "/VsanStorageApplicatorTest_RAID0.json";
    private static final String RAID1 = "/VsanStorageApplicatorTest_RAID1_FTT1.json";

    /**
     * Test the applicator for different settings.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testApplicator() throws Exception {

        // All the settings are off
        Pair<Double, Double> allOff = runApplicator(RAID0, false, 0, 0, 0);
        Assert.assertEquals(32.46, allOff.getFirst(), .01);
        Assert.assertEquals(39.99, allOff.getSecond(), .01);

        // Compression is on
        Pair<Double, Double> comprsession = runApplicator(RAID0, true, 1.5f, 0, 0);
        Assert.assertEquals(48.69, comprsession.getFirst(), .01);
        Assert.assertEquals(59.98, comprsession.getSecond(), .01);

        // Slack is on
        Pair<Double, Double> slack = runApplicator(RAID0, false, 0, 50, 0);
        Assert.assertEquals(32.46, slack.getFirst(), .01);
        Assert.assertEquals(19.99, slack.getSecond(), .01);

        // Slack is on; Compression is on
        Pair<Double, Double> comprsessionAndSlack = runApplicator(RAID0, true, 1.5f, 50, 0);
        Assert.assertEquals(48.69, comprsessionAndSlack.getFirst(), .01);
        Assert.assertEquals(29.99, comprsessionAndSlack.getSecond(), .01);

        // Slack is on; Compression is on; Host capacity is on
        Pair<Double, Double> allOn = runApplicator(RAID0, true, 1.7f, 13, 1);
        Assert.assertEquals(55.18, allOn.getFirst(), .01);
        Assert.assertEquals(44.35, allOn.getSecond(), .01);

        // RAID1; All off
        Pair<Double, Double> raid1AllOff = runApplicator(RAID1, false, 0, 0, 0);
        Assert.assertEquals(16.25, raid1AllOff.getFirst(), .01);
        Assert.assertEquals(19.99, raid1AllOff.getSecond(), .01);

        // RAID1; Compression is on
        Pair<Double, Double> raid1Comprsession = runApplicator(RAID1, true, 1.5f, 0, 0);
        Assert.assertEquals(24.38, raid1Comprsession.getFirst(), .01);
        Assert.assertEquals(29.99, raid1Comprsession.getSecond(), .01);

        // RAID1; Slack is on; Compression is on
        Pair<Double, Double> raid1SlackCompression = runApplicator(RAID1, true, 1.5f, 50, 0);
        Assert.assertEquals(24.38, raid1SlackCompression.getFirst(), .01);
        Assert.assertEquals(15, raid1SlackCompression.getSecond(), .01);

        // RAID1; Host capacity is on
        Pair<Double, Double> raid1Host = runApplicator(RAID1, false, 0, 0, 1);
        Assert.assertEquals(16.25, raid1Host.getFirst(), .01);
        Assert.assertEquals(14.99, raid1Host.getSecond(), .01);

        // RAID1; Host capacity is on; Slack is on
        Pair<Double, Double> raid1HostSlack = runApplicator(RAID1, false, 0, 50, 1);
        Assert.assertEquals(16.25, raid1HostSlack.getFirst(), .01);
        Assert.assertEquals(7.5, raid1HostSlack.getSecond(), .01);

        // RAID1; Host capacity is on; Slack is on; Compression is on
        Pair<Double, Double> raid1HostSlackCompress = runApplicator(RAID1, true, 2.0f, 50, 1);
        Assert.assertEquals(32.5, raid1HostSlackCompress.getFirst(), .01);
        Assert.assertEquals(14.99, raid1HostSlackCompress.getSecond(), .01);

        // RAOD1; All on
        Pair<Double, Double> raid1 = runApplicator(RAID1, true, 1.7f, 13, 1);
        Assert.assertEquals(27.62, raid1.getFirst(), .01);
        Assert.assertEquals(22.18, raid1.getSecond(), .01);
    }

    private static Pair<Double, Double> runApplicator(@Nonnull String fileName,
            boolean hciUseCompression,
            float hciCompressionRatio, float hciSlackSpacePercentage,
            float hciHostCapacityReservation) throws IOException {

        InputStream is = VsanStorageApplicatorTest.class
                .getResourceAsStream(fileName);
        TopologyEntityDTO.Builder storage = TopologyEntityDTO.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(is), storage);

        Map<EntitySettingSpecs, Setting> settings = new HashMap<>();
        settings.put(EntitySettingSpecs.HciUseCompression,
                getBooleanSettingValue(hciUseCompression));
        settings.put(EntitySettingSpecs.HciCompressionRatio,
                getNumericSettingValue(hciCompressionRatio));
        settings.put(EntitySettingSpecs.HciSlackSpacePercentage,
                getNumericSettingValue(hciSlackSpacePercentage));
        settings.put(EntitySettingSpecs.HciHostCapacityReservation,
                getNumericSettingValue(hciHostCapacityReservation));

        new VsanStorageApplicator().apply(storage, settings);

        CommoditySoldDTO.Builder storageAmount = SettingApplicator
                .getCommoditySoldBuilders(storage, CommodityType.STORAGE_AMOUNT).iterator().next();

        return new Pair<>(storageAmount.getUsed() / 1024, storageAmount.getCapacity() / 1024);
    }

    private static Setting getBooleanSettingValue(boolean value) {
        return Setting.newBuilder()
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(value).build())
                .build();
    }

    private static Setting getNumericSettingValue(float value) {
        return Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value).build())
                .build();
    }
}
