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
    private static final int STORAGE_OVERPROVISIONED_PERCENTAGE_DEFAULT = 200;

    private static final int HCI_HOST_CAPACITY_RESERVATION_ON = 1;
    private static final int HCI_HOST_CAPACITY_RESERVATION_OFF = 0;

    /**
     * Test the applicator for different settings.
     *
     * @throws Exception any test exception
     */
    @Test
    public void testApplicator() throws Exception {

        // All the settings are off
        ApplicatorResult allOff = runApplicator(RAID0, false, 0, 0, HCI_HOST_CAPACITY_RESERVATION_OFF);
        allOff.checkStorageAmount(32.46, 39.99);
        allOff.checkIOPS(HCI_HOST_CAPACITY_RESERVATION_OFF);
        allOff.checkUtilizationThreshold(100);

        // Compression is on
        ApplicatorResult comprsession = runApplicator(RAID0, true, 1.5f, 0, HCI_HOST_CAPACITY_RESERVATION_OFF);
        comprsession.checkStorageAmount(48.69, 59.98);
        comprsession.checkUtilizationThreshold(150);

        // Slack is on
        ApplicatorResult slack = runApplicator(RAID0, false, 0, 50, HCI_HOST_CAPACITY_RESERVATION_OFF);
        slack.checkStorageAmount(32.46, 19.99);
        slack.checkUtilizationThreshold(50);

        // Slack is on; Compression is on
        ApplicatorResult comprsessionAndSlack = runApplicator(RAID0, true, 1.5f, 50, HCI_HOST_CAPACITY_RESERVATION_OFF);
        comprsessionAndSlack.checkStorageAmount(48.69, 29.99);
        comprsessionAndSlack.checkUtilizationThreshold(75);

        // Slack is on; Compression is on; Host capacity is on
        ApplicatorResult allOn = runApplicator(RAID0, true, 1.7f, 13, HCI_HOST_CAPACITY_RESERVATION_ON);
        allOn.checkStorageAmount(55.18, 44.35);
        allOn.checkIOPS(HCI_HOST_CAPACITY_RESERVATION_ON);
        allOn.checkUtilizationThreshold(147.9);

        // RAID1; All off
        ApplicatorResult raid1AllOff = runApplicator(RAID1, false, 0, 0, HCI_HOST_CAPACITY_RESERVATION_OFF);
        raid1AllOff.checkStorageAmount(16.25, 10);
        raid1AllOff.checkUtilizationThreshold(50);

        // RAID1; Compression is on
        ApplicatorResult raid1Comprsession = runApplicator(RAID1, true, 1.5f, 0, HCI_HOST_CAPACITY_RESERVATION_OFF);
        raid1Comprsession.checkStorageAmount(24.38, 15);
        raid1Comprsession.checkUtilizationThreshold(75);

        // RAID1; Slack is on; Compression is on
        ApplicatorResult raid1SlackCompression = runApplicator(RAID1, true, 1.5f, 50, HCI_HOST_CAPACITY_RESERVATION_OFF);
        raid1SlackCompression.checkStorageAmount(24.38, 7.5);
        raid1SlackCompression.checkUtilizationThreshold(37.5);

        // RAID1; Host capacity is on
        ApplicatorResult raid1Host = runApplicator(RAID1, false, 0, 0, HCI_HOST_CAPACITY_RESERVATION_ON);
        raid1Host.checkStorageAmount(16.25, 5);
        raid1Host.checkUtilizationThreshold(50);

        // RAID1; Host capacity is on; Slack is on
        ApplicatorResult raid1HostSlack = runApplicator(RAID1, false, 0, 50, HCI_HOST_CAPACITY_RESERVATION_ON);
        raid1HostSlack.checkStorageAmount(16.25, 2.5);
        raid1HostSlack.checkUtilizationThreshold(25);

        // RAID1; Host capacity is on; Slack is on; Compression is on
        ApplicatorResult raid1HostSlackCompress = runApplicator(RAID1, true, 2.0f, 50, HCI_HOST_CAPACITY_RESERVATION_ON);
        raid1HostSlackCompress.checkStorageAmount(32.5, 5);
        raid1HostSlackCompress.checkUtilizationThreshold(50);

        // RAOD1; All on
        ApplicatorResult raid1 = runApplicator(RAID1, true, 1.7f, 13, HCI_HOST_CAPACITY_RESERVATION_ON);
        raid1.checkStorageAmount(27.62, 7.4);
        raid1.checkUtilizationThreshold(73.95);
    }

    private static ApplicatorResult runApplicator(@Nonnull String fileName,
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
        settings.put(EntitySettingSpecs.StorageOverprovisionedPercentage,
                        getNumericSettingValue(STORAGE_OVERPROVISIONED_PERCENTAGE_DEFAULT));

        Pair<GraphWithSettings, Integer> graphNHostsCnt = makeGraphWithSettings(storage, settings);
        new VsanStorageApplicator(graphNHostsCnt.getFirst()).apply(storage, settings);

        return new ApplicatorResult(storage, graphNHostsCnt);
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
                int commodityTypeInt = boughtCommodityDTO.getCommodityType().getType();
                CommoditySoldDTO.Builder soldCommodityBuilder = CommoditySoldDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                                .setType(commodityTypeInt));
                if (commodityTypeInt == CommodityType.STORAGE_AMOUNT_VALUE) {
                    soldCommodityBuilder.setCapacity(boughtCommodityDTO.getUsed());
                    soldCommodityBuilder.setUsed(boughtCommodityDTO.getUsed());
                }
                provider.addCommoditySoldList(soldCommodityBuilder);
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

    /**
     * A class to collect the results of applying {@link VsanStorageApplicator}
     * and run the checking procedures.
     */
    private static class ApplicatorResult  {
        private TopologyEntityDTO.Builder storage;
        private GraphWithSettings graph;
        private int numberOfHosts;

        ApplicatorResult(TopologyEntityDTO.Builder storage,
                        Pair<GraphWithSettings, Integer> graphNHostsCnt)   {
            this.storage = storage;
            this.graph = graphNHostsCnt.getFirst();
            this.numberOfHosts = graphNHostsCnt.getSecond();
        }

        public void checkStorageAmount(double expectedUsed, double expectedCapacity)    {
            CommoditySoldDTO.Builder storageAmount = SettingApplicator
                            .getCommoditySoldBuilders(storage, CommodityType.STORAGE_AMOUNT)
                            .iterator().next();
            Assert.assertEquals(expectedUsed, storageAmount.getUsed() / 1024, .01);
            Assert.assertEquals(expectedCapacity, storageAmount.getCapacity() / 1024, .01);
        }

        public void checkIOPS(int hciHostCapacityReservation)   {
            CommoditySoldDTO.Builder storageAccess = SettingApplicator
                            .getCommoditySoldBuilders(storage, CommodityType.STORAGE_ACCESS)
                            .iterator().next();
            int referenceIOPSCapacity = (numberOfHosts - hciHostCapacityReservation)
                            * HCI_HOST_IOPS_CAPACITY_DEFAULT;
            Assert.assertEquals(referenceIOPSCapacity, (int)storageAccess.getCapacity());
        }

        public void checkUtilizationThreshold(double hciUsablePercentage)    {
            for (CommoditiesBoughtFromProvider commoditiesFromProvider
                            : storage.getCommoditiesBoughtFromProvidersList())   {
                if (commoditiesFromProvider.getProviderEntityType()
                                != EntityType.PHYSICAL_MACHINE_VALUE)    {
                    continue;
                }
                long providerId = commoditiesFromProvider.getProviderId();
                TopologyEntity provider = graph.getTopologyGraph().getEntity(providerId).get();
                for (CommoditySoldDTO.Builder soldCommodity
                                : provider.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()) {
                    int commodityTypeInt = soldCommodity.getCommodityType().getType();
                    if (commodityTypeInt != CommodityType.STORAGE_PROVISIONED_VALUE
                                    && commodityTypeInt != CommodityType.STORAGE_AMOUNT_VALUE)  {
                        continue;
                    }
                    Assert.assertEquals(hciUsablePercentage, soldCommodity.getEffectiveCapacityPercentage(), .01);
                }
            }
        }
    }
}
