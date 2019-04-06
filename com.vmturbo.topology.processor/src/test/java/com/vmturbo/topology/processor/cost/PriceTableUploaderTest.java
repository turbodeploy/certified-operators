package com.vmturbo.topology.processor.cost;

import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceImplBase;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.ComputePriceTableByTierEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.DatabasePriceTableByTierEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.StoragePriceTableByTierEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 *
 */
public class PriceTableUploaderTest {
    // test GRPC server
    private final TestPriceService priceServiceSpy = spy(new TestPriceService());

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(priceServiceSpy);

    // test cost component client
    private PricingServiceStub priceServiceClient;

    private TargetStore targetStore = Mockito.mock(TargetStore.class);

    Map<String,Long> cloudOidByLocalId;

    private PriceTableUploader priceTableUploader;

    @Before
    public void setup() {
        cloudOidByLocalId = new HashMap<>();
        cloudOidByLocalId.put("region-1", 1L);
        cloudOidByLocalId.put("compute-tier-1", 10L);
        cloudOidByLocalId.put("db-tier-1", 20L);
        cloudOidByLocalId.put("aws::ST::STORAGE-TIER-1", 30L);
    }

    @Test
    public void testComputeOnDemand() {
        // build a set of cost data that the price tables will be built from
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                    .setRelatedRegion(EntityDTO.newBuilder()
                            .setId("region-1")
                            .setDisplayName("region-1")
                            .setEntityType(EntityType.REGION))
                    .addComputePriceTable(ComputePriceTableByTierEntry.newBuilder()
                            .setRelatedComputeTier(EntityDTO.newBuilder()
                                    .setId("compute-tier-1")
                                    .setDisplayName("compute-tier-1")
                                    .setEntityType(EntityType.COMPUTE_TIER))
                            .setComputeTierPriceList(ComputeTierPriceList.newBuilder()
                                    .setBasePrice(ComputeTierConfigPrice.newBuilder()
                                            .setGuestOsType(OSType.RHEL)
                                            .addPrices(Price.newBuilder()
                                                    .setPriceAmount(CurrencyAmount.newBuilder()
                                                            .setAmount(1.0)))))))
                .build();

        // call the price table builder
        priceTableUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(), 100);
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable, cloudOidByLocalId, SDKProbeType.AWS_COST);
        // check the results.
        Assert.assertEquals(1, priceTable.getOnDemandPriceByRegionIdCount());
        // should have an entry for region 1
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(1L));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(1L);
        Assert.assertTrue(onDemandTable.containsComputePricesByTierId(10L));
        ComputeTierPriceList computeTierPriceList = onDemandTable.getComputePricesByTierIdMap().get(10L);
        Assert.assertEquals(OSType.RHEL, computeTierPriceList.getBasePrice().getGuestOsType());
        Assert.assertEquals(1.0,computeTierPriceList.getBasePrice().getPrices(0).getPriceAmount().getAmount(),0);
    }

    @Test
    public void testDatabaseOnDemand() {
        // build a set of cost data that the price tables will be built from
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                        .setRelatedRegion(EntityDTO.newBuilder()
                                .setId("region-1")
                                .setDisplayName("region-1")
                                .setEntityType(EntityType.REGION))
                        .addDatabasePriceTable(DatabasePriceTableByTierEntry.newBuilder()
                                .setRelatedDatabaseTier(EntityDTO.newBuilder()
                                        .setId("db-tier-1")
                                        .setDisplayName("db-tier-1")
                                        .setEntityType(EntityType.DATABASE_TIER))
                                .setDatabaseTierPriceList(DatabaseTierPriceList.newBuilder()
                                        .setBasePrice(DatabaseTierConfigPrice.newBuilder()
                                                .setDbEdition(DatabaseEdition.ORACLE_ENTERPRISE)
                                                .setDbEngine(DatabaseEngine.ORACLE)
                                                .addPrices(Price.newBuilder()
                                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                                .setAmount(2.0)))))))
                .build();

        // call the price table builder
        priceTableUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(), 100);
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable, cloudOidByLocalId, SDKProbeType.AWS_COST);

        // check the results.
        Assert.assertEquals(1, priceTable.getOnDemandPriceByRegionIdCount());
        // should have an entry for region 1
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(1L));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(1L);
        Assert.assertTrue(onDemandTable.containsDbPricesByInstanceId(20L));
        DatabaseTierPriceList dbPriceList = onDemandTable.getDbPricesByInstanceIdMap().get(20L);
        Assert.assertEquals(DatabaseEdition.ORACLE_ENTERPRISE, dbPriceList.getBasePrice().getDbEdition());
        Assert.assertEquals(2.0,dbPriceList.getBasePrice().getPrices(0).getPriceAmount().getAmount(),0);
    }

    @Test
    public void testStorageOnDemand() {
        // build a set of cost data that the price tables will be built from
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                        .setRelatedRegion(EntityDTO.newBuilder()
                                .setId("region-1")
                                .setDisplayName("region-1")
                                .setEntityType(EntityType.REGION))
                        .addStoragePriceTable(StoragePriceTableByTierEntry.newBuilder()
                                .setRelatedStorageTier(EntityDTO.newBuilder()
                                        .setId("storage-tier-1")
                                        .setDisplayName("storage-tier-1")
                                        .setEntityType(EntityType.STORAGE_TIER))
                                .setStorageTierPriceList(StorageTierPriceList.newBuilder()
                                        .addCloudStoragePrice(StorageTierPrice.newBuilder()
                                                .addPrices(Price.newBuilder()
                                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                                .setAmount(10)))))))
                .build();
        // call the price table builder
        priceTableUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(), 100);
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable, cloudOidByLocalId, SDKProbeType.AWS_COST);
        // check the results.
        Assert.assertEquals(1, priceTable.getOnDemandPriceByRegionIdCount());
        // should have an entry for region 1
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(1L));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(1L);
        Assert.assertTrue(onDemandTable.containsCloudStoragePricesByTierId(30L));
        StorageTierPriceList storageTierPriceList = onDemandTable.getCloudStoragePricesByTierIdMap().get(30L);
        Assert.assertEquals(10, storageTierPriceList.getCloudStoragePriceList().get(0).getPrices(0).getPriceAmount().getAmount(), 0);
    }

    public static class TestPriceService extends PricingServiceImplBase {
    }
}
