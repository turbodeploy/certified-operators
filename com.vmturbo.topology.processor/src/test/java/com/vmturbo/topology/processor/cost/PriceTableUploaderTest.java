package com.vmturbo.topology.processor.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
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
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
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
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList.IpConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceByOsEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceByOsEntry.LicensePrice;
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

    private static final double RHEL_LICENSE_PRICE = 5.1;
    private static final double DELTA = 1e-10;
    private static final String REGION_ONE = "region-1";
    private static final Long INDEX_ONE = 1L;
    private static final double IP_RPICE_AMOUNT = 0.5;

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
        cloudOidByLocalId.put(REGION_ONE, 1L);
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
                            .setId(REGION_ONE)
                            .setDisplayName(REGION_ONE)
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
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(INDEX_ONE));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(INDEX_ONE);
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
                                .setId(REGION_ONE)
                                .setDisplayName(REGION_ONE)
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
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(INDEX_ONE));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(INDEX_ONE);
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
                                .setId(REGION_ONE)
                                .setDisplayName(REGION_ONE)
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
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(INDEX_ONE));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(INDEX_ONE);
        Assert.assertTrue(onDemandTable.containsCloudStoragePricesByTierId(30L));
        StorageTierPriceList storageTierPriceList = onDemandTable.getCloudStoragePricesByTierIdMap().get(30L);
        Assert.assertEquals(10, storageTierPriceList.getCloudStoragePriceList().get(0).getPrices(0).getPriceAmount().getAmount(), 0);
    }

    /**
     * Verify that license costs are uploaded properly to the Price Table used by cost component.
     */
    @Test
    public void testLicensePrices() {
        // Build a set of cost data that the license price table will be built from
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addLicensePriceTable(createLicensePriceByOsEntry(OSType.RHEL, 4, RHEL_LICENSE_PRICE))
                .build();

        // The third argument of the uploader is riSpecPriceChunkSize which is irrelevant here
        priceTableUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(), 100);
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable,
                cloudOidByLocalId, SDKProbeType.AZURE_COST);
        // should have an entry for RHEL licenses
        Assert.assertEquals(1, priceTable.getLicensePricesCount());
        Assert.assertEquals(OSType.RHEL, priceTable.getLicensePricesList().get(0).getOsType());
        LicensePriceByOsEntry rhelEntry = priceTable.getLicensePricesList().get(0);
        Assert.assertEquals(RHEL_LICENSE_PRICE, rhelEntry.getLicensePrices(0).getPrice()
                .getPriceAmount().getAmount(), DELTA);
    }

    /**
     * Create a {@link LicensePriceByOsEntry}.
     *
     * @param os the {@link OSType} to create the entry for
     * @param numOfCores numner of cores for which the license price is given
     * @param amount the price of the created license
     * @return {@link LicensePriceByOsEntry}
     */
    private LicensePriceByOsEntry createLicensePriceByOsEntry(OSType os, int numOfCores, double amount) {
        return LicensePriceByOsEntry.newBuilder()
                .setOsType(os)
                .addLicensePrices(LicensePrice.newBuilder()
                        .setNumberOfCores(numOfCores)
                        .setPrice(Price.newBuilder()
                                .setPriceAmount(CurrencyAmount.newBuilder()
                                        .setAmount(amount)))).build();
    }

    /**
     * Verify that ip costs are uploaded properly to the Price Table used by cost component.
     */
    @Test
    public void testIpPrices() {
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
            .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                .setRelatedRegion(EntityDTO.newBuilder()
                    .setId(REGION_ONE)
                    .setDisplayName(REGION_ONE)
                    .setEntityType(EntityType.REGION))
                .setIpPrices(IpPriceList.newBuilder()
                    .addIpPrice(IpConfigPrice.newBuilder().addPrices(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder()
                            .setAmount(IP_RPICE_AMOUNT))))))
            .build();
        priceTableUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(), 100);
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable,
            cloudOidByLocalId, SDKProbeType.AZURE_COST);
        OnDemandPriceTable onDemandPriceTable = priceTable.getOnDemandPriceByRegionIdMap().get(INDEX_ONE);
        Assert.assertNotNull(onDemandPriceTable);
        Assert.assertEquals(IP_RPICE_AMOUNT, onDemandPriceTable.getIpPrices().getIpPrice(0)
            .getPrices(0).getPriceAmount().getAmount(), DELTA);
    }

    @Test
    public void testDiags() throws DiagnosticsException {
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
            .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                .setRelatedRegion(EntityDTO.newBuilder()
                    .setId(REGION_ONE)
                    .setDisplayName(REGION_ONE)
                    .setEntityType(EntityType.REGION))
                .setIpPrices(IpPriceList.newBuilder()
                    .addIpPrice(IpConfigPrice.newBuilder().addPrices(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder()
                            .setAmount(IP_RPICE_AMOUNT))))))
            .build();

        priceTableUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(), 100);
        priceTableUploader.recordPriceTable(SDKProbeType.AZURE_COST, sourcePriceTable);

        final Map<SDKProbeType, PricingDTO.PriceTable> originalSrcTables = priceTableUploader.getSourcePriceTables();
        assertThat(originalSrcTables.keySet(), containsInAnyOrder(SDKProbeType.AZURE_COST));
        assertThat(originalSrcTables.get(SDKProbeType.AZURE_COST), is(sourcePriceTable));

        // ACT
        List<String> diags = priceTableUploader.collectDiags();

        PriceTableUploader newUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(), 100);
        newUploader.restoreDiags(diags);
        final Map<SDKProbeType, PricingDTO.PriceTable> newSrcTables = newUploader.getSourcePriceTables();

        assertThat(newSrcTables, is(originalSrcTables));
    }

    public static class TestPriceService extends PricingServiceImplBase {
    }
}
