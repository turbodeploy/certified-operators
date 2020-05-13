package com.vmturbo.topology.processor.group.settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Tests for class VsanStorageApplicator.
 */
public class VsanStorageApplicatorTest {

    private static final String RAID0 = "/VsanStorageApplicatorTest_RAID0.json";
    private static final String RAID1 = "/VsanStorageApplicatorTest_RAID1_FTT1.json";

    private static final int HCI_HOST_IOPS_CAPACITY_DEFAULT = 50000;

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
        Assert.assertEquals(10, raid1AllOff.getSecond(), .01);

        // RAID1; Compression is on
        Pair<Double, Double> raid1Comprsession = runApplicator(RAID1, true, 1.5f, 0, 0);
        Assert.assertEquals(24.38, raid1Comprsession.getFirst(), .01);
        Assert.assertEquals(15, raid1Comprsession.getSecond(), .01);

        // RAID1; Slack is on; Compression is on
        Pair<Double, Double> raid1SlackCompression = runApplicator(RAID1, true, 1.5f, 50, 0);
        Assert.assertEquals(24.38, raid1SlackCompression.getFirst(), .01);
        Assert.assertEquals(7.5, raid1SlackCompression.getSecond(), .01);

        // RAID1; Host capacity is on
        Pair<Double, Double> raid1Host = runApplicator(RAID1, false, 0, 0, 1);
        Assert.assertEquals(16.25, raid1Host.getFirst(), .01);
        Assert.assertEquals(5, raid1Host.getSecond(), .01);

        // RAID1; Host capacity is on; Slack is on
        Pair<Double, Double> raid1HostSlack = runApplicator(RAID1, false, 0, 50, 1);
        Assert.assertEquals(16.25, raid1HostSlack.getFirst(), .01);
        Assert.assertEquals(2.5, raid1HostSlack.getSecond(), .01);

        // RAID1; Host capacity is on; Slack is on; Compression is on
        Pair<Double, Double> raid1HostSlackCompress = runApplicator(RAID1, true, 2.0f, 50, 1);
        Assert.assertEquals(32.5, raid1HostSlackCompress.getFirst(), .01);
        Assert.assertEquals(5, raid1HostSlackCompress.getSecond(), .01);

        // RAOD1; All on
        Pair<Double, Double> raid1 = runApplicator(RAID1, true, 1.7f, 13, 1);
        Assert.assertEquals(27.62, raid1.getFirst(), .01);
        Assert.assertEquals(7.4, raid1.getSecond(), .01);
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
        settings.put(EntitySettingSpecs.HciHostIopsCapacity,
                getNumericSettingValue(HCI_HOST_IOPS_CAPACITY_DEFAULT));

        Pair<GraphWithSettings, Integer> graphNHostsCnt = makeGraphWithSettings(storage, settings);
        new VsanStorageApplicator(graphNHostsCnt.getFirst()).apply(storage, settings);

        CommoditySoldDTO.Builder storageAccess = SettingApplicator
                        .getCommoditySoldBuilders(storage, CommodityType.STORAGE_ACCESS).iterator().next();
        int referenceIOPSCapacity = (int)((graphNHostsCnt.getSecond() -
                        hciHostCapacityReservation) * HCI_HOST_IOPS_CAPACITY_DEFAULT);
        Assert.assertEquals(referenceIOPSCapacity, (int)storageAccess.getCapacity());


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

    private static Pair<GraphWithSettings, Integer> makeGraphWithSettings(TopologyEntityDTO.Builder storage,
                    Map<EntitySettingSpecs, Setting> storageSettings)    {
        int cntHosts = 0;
        Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        for (CommoditiesBoughtFromProvider commoditiesFromProvider :
                storage.getCommoditiesBoughtFromProvidersList())   {
            if (commoditiesFromProvider.getProviderEntityType() !=
                            EntityType.PHYSICAL_MACHINE_VALUE)    {
                continue;
            }
            long providerId = commoditiesFromProvider.getProviderId();
            TopologyEntityDTO.Builder provider = TopologyEntityDTO.newBuilder()
                            .setOid(providerId)
                            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE);
            for (CommodityBoughtDTO boughtCommodityDTO : commoditiesFromProvider
                            .getCommodityBoughtList())  {
                if (boughtCommodityDTO.getCommodityType().getType() !=
                                CommodityType.STORAGE_ACCESS_VALUE) {
                    continue;
                }
                provider.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                                .setType(CommodityType.STORAGE_ACCESS_VALUE)));
            }

            if (provider.getCommoditySoldListCount() != 0)  {
                cntHosts++;
                topology.put(providerId, TopologyEntity.newBuilder(provider));
            }
        }
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator
                        .newGraph(topology);

        long storageId = storage.getOid();
        EntitySettings.Builder settingsBuilder = EntitySettings.newBuilder()
                        .setEntityOid(storageId);
        for (Map.Entry<EntitySettingSpecs, Setting> aStorageSetting : storageSettings.entrySet())   {
            Setting setting = Setting.newBuilder(aStorageSetting.getValue())
                            .setSettingSpecName(aStorageSetting.getKey().getSettingName())
                            .build();
            settingsBuilder.addUserSettings(SettingToPolicyId.newBuilder()
                            .setSetting(setting).addSettingPolicyId(1L));
        }
        Map<Long, EntitySettings> entitySettings = ImmutableMap.of(
                        storageId, settingsBuilder.build());

        final SettingPolicy policy = SettingPolicy.newBuilder().setId(storageId).build();
        //Policy ID doesn't really matter for our purposes.
        GraphWithSettings result = new GraphWithSettings(graph, entitySettings,
                        Collections.singletonMap(storageId, policy));
        return new Pair<>(result, cntHosts);
    }
}
