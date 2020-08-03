package com.vmturbo.topology.processor.cost;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import io.grpc.Channel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstanceSpecPrice;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceImplBase;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceSpec;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList.IpConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.ComputePriceTableByTierEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.DatabasePriceTableByTierEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry.StoragePriceTableByTierEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.ReservedInstancePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.ReservedInstancePriceTableByRegionEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.cost.PriceTableUploader.ProbePriceData;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Unit tests for {@link PriceTableUploader}.
 */
public class PriceTableUploaderTest {

    private static final double RHEL_LICENSE_PRICE = 5.1;
    private static final double WINDOWS_LICENSE_PRICE = 4.5;
    private static final double DELTA = 1e-10;
    private static final String REGION_ONE = "region-1";
    private static final String COMPUTE_TIER_ONE = "compute-tier-1";
    private static final String DB_TIER_ONE = "db-tier-1";
    private static final String STORAGE_TIER_ONE = "storage-tier-1";
    private static final String AWS_STORAGE_TIER_ONE = "aws::ST::STORAGE-TIER-1";
    private static final String PROBE_TYPE_FIELD = "probeType";
    private static final Long INDEX_ONE = 1L;
    private static final double IP_PRICE_AMOUNT = 0.5;
    private static final Long TARGET_ID  = 0L;

    private static final Builder REGION_ENTITY_BUILDER =
        createBuilder(REGION_ONE, REGION_ONE, EntityType.REGION);
    private static final Builder COMPUTE_TIER_BUILDER =
        createBuilder(COMPUTE_TIER_ONE, COMPUTE_TIER_ONE, EntityType.COMPUTE_TIER);
    private static final Builder DB_TIER_BUILDER =
        createBuilder(DB_TIER_ONE, DB_TIER_ONE, EntityType.DATABASE_TIER);
    private static final Builder STORAGE_TIER_BUILDER =
        createBuilder(STORAGE_TIER_ONE, STORAGE_TIER_ONE, EntityType.STORAGE_TIER);

    // test GRPC server
    private final TestPriceService priceServiceSpy = spy(new TestPriceService());

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(priceServiceSpy);

    // test cost component client
    private PricingServiceStub priceServiceClient = PricingServiceGrpc.newStub(mock(Channel.class));

    private TargetStore targetStore = mock(TargetStore.class);

    private CloudEntitiesMap cloudEntitiesMap = mock(CloudEntitiesMap.class);

    private Map<String, Long> cloudOidByLocalId;

    private SpotPriceTableConverter spotPriceTableConverter;

    private PriceTableUploader priceTableUploader;

    @Before
    public void setup() {
        cloudOidByLocalId = new HashMap<>();
        cloudOidByLocalId.put(REGION_ONE, 1L);
        cloudOidByLocalId.put(COMPUTE_TIER_ONE, 10L);
        cloudOidByLocalId.put(DB_TIER_ONE, 20L);
        cloudOidByLocalId.put(AWS_STORAGE_TIER_ONE, 30L);
        spotPriceTableConverter = spy(SpotPriceTableConverter.class);
        priceTableUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(),
                100, targetStore, spotPriceTableConverter);
        when(targetStore.getProbeTypeForTarget(TARGET_ID)).thenReturn(Optional.of(SDKProbeType.AWS));
    }

    private static Builder createBuilder(String id, String displayName, EntityType entityType) {
        return EntityDTO.newBuilder()
            .setId(id)
            .setDisplayName(displayName)
            .setEntityType(entityType);
    }

    @Test
    public void testComputeOnDemand() {
        // build a set of cost data that the price tables will be built from
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                    .setRelatedRegion(REGION_ENTITY_BUILDER)
                    .addComputePriceTable(ComputePriceTableByTierEntry.newBuilder()
                            .setRelatedComputeTier(COMPUTE_TIER_BUILDER)
                            .setComputeTierPriceList(ComputeTierPriceList.newBuilder()
                                    .setBasePrice(ComputeTierConfigPrice.newBuilder()
                                            .setGuestOsType(OSType.RHEL)
                                            .addPrices(Price.newBuilder()
                                                    .setPriceAmount(CurrencyAmount.newBuilder()
                                                            .setAmount(1.0)))))))
                .build();
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable, cloudOidByLocalId, cloudEntitiesMap.getExtraneousIdLookUps(), SDKProbeType.AWS_COST);
        verify(spotPriceTableConverter, times(1)).convertSpotPrices(any(), any());
        // check the results.
        Assert.assertEquals(1, priceTable.getOnDemandPriceByRegionIdCount());
        // should have an entry for region 1
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(INDEX_ONE));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(INDEX_ONE);
        long computeEntryId = cloudOidByLocalId.get(COMPUTE_TIER_ONE);
        Assert.assertTrue(onDemandTable.containsComputePricesByTierId(computeEntryId));
        ComputeTierPriceList computeTierPriceList = onDemandTable.getComputePricesByTierIdMap().get(computeEntryId);
        Assert.assertEquals(OSType.RHEL, computeTierPriceList.getBasePrice().getGuestOsType());
        Assert.assertEquals(1.0,computeTierPriceList.getBasePrice().getPrices(0).getPriceAmount().getAmount(),0);
    }

    @Test
    public void testDatabaseOnDemand() {
        // build a set of cost data that the price tables will be built from
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                        .setRelatedRegion(REGION_ENTITY_BUILDER)
                        .addDatabasePriceTable(DatabasePriceTableByTierEntry.newBuilder()
                                .setRelatedDatabaseTier(DB_TIER_BUILDER)
                                .addDatabaseTierPriceList(DatabaseTierPriceList.newBuilder()
                                        .setDeploymentType(CloudCostDTO.DeploymentType.SINGLE_AZ)
                                        .setBasePrice(DatabaseTierConfigPrice.newBuilder()
                                                .setDbEdition(DatabaseEdition.ENTERPRISE)
                                                .setDbEngine(DatabaseEngine.ORACLE)
                                                .setDbDeploymentType(CloudCostDTO.DeploymentType.SINGLE_AZ)
                                                .setDbLicenseModel(CloudCostDTO.LicenseModel.LICENSE_INCLUDED)
                                                .addPrices(Price.newBuilder()
                                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                                .setAmount(2.0)))))))
                .build();
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable, cloudOidByLocalId, cloudEntitiesMap.getExtraneousIdLookUps(), SDKProbeType.AWS_COST);
        // check the results.
        Assert.assertEquals(1, priceTable.getOnDemandPriceByRegionIdCount());
        // should have an entry for region 1
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(INDEX_ONE));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(INDEX_ONE);
        long dbId = cloudOidByLocalId.get(DB_TIER_ONE);
        Assert.assertTrue(onDemandTable.containsDbPricesByInstanceId(dbId));
        DatabaseTierPriceList dbPriceList = onDemandTable.getDbPricesByInstanceIdMap().get(dbId)
            .getDbPricesByDeploymentTypeOrDefault(CloudCostDTO.DeploymentType.SINGLE_AZ.getNumber(), null);
        Assert.assertEquals(DatabaseEdition.ENTERPRISE, dbPriceList.getBasePrice().getDbEdition());
        Assert.assertEquals(2.0,dbPriceList.getBasePrice().getPrices(0).getPriceAmount().getAmount(),0);
    }

    @Test
    public void testStorageOnDemand() {
        // build a set of cost data that the price tables will be built from
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                        .setRelatedRegion(REGION_ENTITY_BUILDER)
                        .addStoragePriceTable(StoragePriceTableByTierEntry.newBuilder()
                                .setRelatedStorageTier(STORAGE_TIER_BUILDER)
                                .setStorageTierPriceList(StorageTierPriceList.newBuilder()
                                        .addCloudStoragePrice(StorageTierPrice.newBuilder()
                                                .addPrices(Price.newBuilder()
                                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                                .setAmount(10)))))))
                .build();
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable, cloudOidByLocalId, cloudEntitiesMap.getExtraneousIdLookUps(), SDKProbeType.AWS_COST);
        // check the results.
        Assert.assertEquals(1, priceTable.getOnDemandPriceByRegionIdCount());
        // should have an entry for region 1
        Assert.assertTrue(priceTable.getOnDemandPriceByRegionIdMap().containsKey(INDEX_ONE));
        OnDemandPriceTable onDemandTable = priceTable.getOnDemandPriceByRegionIdMap().get(INDEX_ONE);
        long stsId = cloudOidByLocalId.get(AWS_STORAGE_TIER_ONE);
        Assert.assertTrue(onDemandTable.containsCloudStoragePricesByTierId(stsId));
        StorageTierPriceList storageTierPriceList = onDemandTable.getCloudStoragePricesByTierIdMap().get(stsId);
        Assert.assertEquals(10, storageTierPriceList.getCloudStoragePriceList().get(0).getPrices(0).getPriceAmount().getAmount(), 0);
    }

    /**
     * Verify that license costs are uploaded properly to the Price Table used by cost component.
     */
    @Test
    public void testLicensePrices() {
        // Build a set of cost data that the license price table will be built from
        LicensePriceEntry rhelLicense = createLicensePriceEntry(OSType.RHEL, 4, RHEL_LICENSE_PRICE);
        LicensePriceEntry windowsLicense = createLicensePriceEntry(OSType.WINDOWS_SERVER, 4, WINDOWS_LICENSE_PRICE);
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addOnDemandLicensePriceTable(rhelLicense).addReservedLicensePriceTable(rhelLicense)
                        .addReservedLicensePriceTable(windowsLicense)
                .build();
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable,
                cloudOidByLocalId, cloudEntitiesMap.getExtraneousIdLookUps(), SDKProbeType.AZURE_COST);
        // should have an entry for RHEL licenses
        Assert.assertEquals(1, priceTable.getOnDemandLicensePricesCount());
        Assert.assertEquals(OSType.RHEL, priceTable.getOnDemandLicensePricesList().get(0).getOsType());
        LicensePriceEntry rhelEntry = priceTable.getOnDemandLicensePricesList().get(0);
        Assert.assertEquals(RHEL_LICENSE_PRICE, rhelEntry.getLicensePrices(0).getPrice()
                .getPriceAmount().getAmount(), DELTA);
        // should have 2 entries including one from Windows.
        Assert.assertEquals(2, priceTable.getReservedLicensePricesCount());
        Assert.assertEquals(OSType.WINDOWS_SERVER, priceTable.getReservedLicensePricesList().get(1).getOsType());
        LicensePriceEntry windowsEntry = priceTable.getReservedLicensePricesList().get(1);
        Assert.assertEquals(WINDOWS_LICENSE_PRICE, windowsEntry.getLicensePrices(0).getPrice()
                        .getPriceAmount().getAmount(), DELTA);
    }

    /**
     * Create a {@link LicensePriceEntry}.
     *
     * @param os the {@link OSType} to create the entry for
     * @param numOfCores number of cores for which the license price is given
     * @param amount the price of the created license
     * @return {@link LicensePriceEntry}
     */
    private LicensePriceEntry createLicensePriceEntry(OSType os, int numOfCores, double amount) {
        return LicensePriceEntry.newBuilder()
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
                .setRelatedRegion(REGION_ENTITY_BUILDER)
                .setIpPrices(IpPriceList.newBuilder()
                    .addIpPrice(IpConfigPrice.newBuilder().addPrices(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder()
                            .setAmount(IP_PRICE_AMOUNT))))))
            .build();
        PriceTable priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable,
            cloudOidByLocalId, cloudEntitiesMap.getExtraneousIdLookUps(), SDKProbeType.AZURE_COST);
        OnDemandPriceTable onDemandPriceTable = priceTable.getOnDemandPriceByRegionIdMap().get(INDEX_ONE);
        Assert.assertNotNull(onDemandPriceTable);
        Assert.assertEquals(IP_PRICE_AMOUNT, onDemandPriceTable.getIpPrices().getIpPrice(0)
            .getPrices(0).getPriceAmount().getAmount(), DELTA);
    }

    /**
     * Verify that RI coverage is uploaded properly to the Price Table used by cost component.
     * That means it will have a valid region, tier and no conflicts with other ri specs.
     */
    @Test
    public void testRiCoverage() {
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
            .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                .setRelatedRegion(REGION_ENTITY_BUILDER)
                .addComputePriceTable(ComputePriceTableByTierEntry.newBuilder()
                    .setRelatedComputeTier(COMPUTE_TIER_BUILDER)
                    .setComputeTierPriceList(ComputeTierPriceList.newBuilder()
                        .setBasePrice(ComputeTierConfigPrice.newBuilder()
                            .setGuestOsType(OSType.RHEL)
                            .addPrices(Price.newBuilder()
                                .setPriceAmount(CurrencyAmount.newBuilder()
                                    .setAmount(1.0)))))))
            // Valid spec
            .addReservedInstancePriceTable(createReservedInstancePriceTable(REGION_ENTITY_BUILDER,
                EntityType.VIRTUAL_MACHINE, COMPUTE_TIER_ONE))
            // Conflicting spec
            .addReservedInstancePriceTable(createReservedInstancePriceTable(REGION_ENTITY_BUILDER,
                EntityType.VIRTUAL_MACHINE, COMPUTE_TIER_ONE))
            // Missing region spec
            .addReservedInstancePriceTable(createReservedInstancePriceTable(createBuilder("2",
                "2", EntityType.REGION), EntityType.VIRTUAL_MACHINE, COMPUTE_TIER_ONE))
            // No region spec
            .addReservedInstancePriceTable(createReservedInstancePriceTableNoRegion(REGION_ENTITY_BUILDER,
                EntityType.VIRTUAL_MACHINE, COMPUTE_TIER_ONE))
            // No tier spec
            .addReservedInstancePriceTable(createReservedInstancePriceTableNoTier(REGION_ENTITY_BUILDER))
            .build();
        final List<ReservedInstanceSpecPrice> riSpecPrices =
            PriceTableUploader.getRISpecPrices(sourcePriceTable, cloudOidByLocalId);
        Assert.assertEquals(1, riSpecPrices.size());
        Assert.assertEquals((long)cloudOidByLocalId.get(COMPUTE_TIER_ONE),
            riSpecPrices.get(0).getRiSpecInfo().getTierId());
    }

    @Test
    public void testDiags() throws DiagnosticsException {
        final long anotherTargetId  = 1L;
        PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
            .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                .setRelatedRegion(REGION_ENTITY_BUILDER)
                .setIpPrices(IpPriceList.newBuilder()
                    .addIpPrice(IpConfigPrice.newBuilder().addPrices(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder()
                            .setAmount(IP_PRICE_AMOUNT))))))
            .build();
        priceTableUploader.recordPriceTable(TARGET_ID, SDKProbeType.AZURE_COST,
                Optional.of(ProbeCategory.COST), sourcePriceTable);
        priceTableUploader.recordPriceTable(anotherTargetId, SDKProbeType.AWS_COST,
                Optional.of(ProbeCategory.COST), sourcePriceTable);

        final Map<Long, PricingDTO.PriceTable> originalSrcTables = priceTableUploader.getSourcePriceTables();
        assertThat(originalSrcTables.keySet(), containsInAnyOrder(TARGET_ID, anotherTargetId ));
        assertThat(originalSrcTables.get(TARGET_ID), is(sourcePriceTable));

        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        // ACT
        priceTableUploader.collectDiags(appender);
        final ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(argumentCaptor.capture());

        PriceTableUploader newUploader = new PriceTableUploader(priceServiceClient, Clock.systemUTC(),
                100, targetStore, spotPriceTableConverter);
        newUploader.restoreDiags(argumentCaptor.getAllValues());
        final Map<Long, PricingDTO.PriceTable> newSrcTables = newUploader.getSourcePriceTables();

        assertThat(newSrcTables, is(originalSrcTables));
    }

    /**
     * Verify that targets are added and removed properly.
     */
    @Test
    public void testTargetRemoved() {
        PricingDTO.PriceTable sourcePriceTable = createDefaultPriceTable();
        // Trying to record price with a non cost probe
        priceTableUploader.recordPriceTable(TARGET_ID, SDKProbeType.AZURE, Optional.of(ProbeCategory.CLOUD_MANAGEMENT),
                sourcePriceTable);
        Assert.assertEquals(0, priceTableUploader.getSourcePriceTables().size());
        // Trying to record price with a cost probe
        priceTableUploader.recordPriceTable(TARGET_ID, SDKProbeType.AZURE_COST,
                Optional.of(ProbeCategory.COST), sourcePriceTable);
        Assert.assertEquals(1, priceTableUploader.getSourcePriceTables().size());
        // Trying to remove an invalid target
        priceTableUploader.targetRemoved(TARGET_ID, SDKProbeType.AWS, ProbeCategory.CLOUD_MANAGEMENT);
        Assert.assertEquals(1, priceTableUploader.getSourcePriceTables().size());
        // Trying to remove a valid target
        priceTableUploader.targetRemoved(TARGET_ID, SDKProbeType.AZURE_COST, ProbeCategory.COST);
        Assert.assertEquals(0, priceTableUploader.getSourcePriceTables().size());
    }

    /**
     * Verify that prices are being built correctly.
     */
    @Test
    public void testBuildPricesToUpload() {
        TopologyProcessorCostTestUtils utils = new TopologyProcessorCostTestUtils();
        final StitchingContext stitchingContext = utils.setupStitchingContext();
        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(stitchingContext, new HashMap<>());
        PricingDTO.PriceTable sourcePriceTable = createDefaultPriceTable();
        // Adding the price table that will be tested
        priceTableUploader.recordPriceTable(TARGET_ID, SDKProbeType.AZURE_COST,
                Optional.of(ProbeCategory.COST), sourcePriceTable);
        Map<Long, SDKProbeType> probeTypesForTargetId = Maps.newHashMap(
                ImmutableMap.of(TARGET_ID, SDKProbeType.AZURE_COST));
        // Triggering 'buildPricesToUpload' method a few time instead of saving re result in order
        // to no add a mvn dependency for ProbePriceData
        ImmutableMap<Long, ProbePriceData> checksumToProbePriceData = ImmutableMap.of(0L,
                priceTableUploader.buildPricesToUpload(probeTypesForTargetId, cloudEntitiesMap).get(0));
        priceTableUploader.uploadPrices(checksumToProbePriceData);
        Assert.assertEquals(1, priceTableUploader.buildPricesToUpload(probeTypesForTargetId, cloudEntitiesMap).size());
        String probeType = (String)Whitebox.getInternalState(priceTableUploader
            .buildPricesToUpload(probeTypesForTargetId, cloudEntitiesMap).get(0), PROBE_TYPE_FIELD);
        Assert.assertEquals(SDKProbeType.AZURE_COST.getProbeType(), probeType);
    }


    @Test
    public void checkProbePriceDataEquals() {
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                        .setRelatedRegion(REGION_ENTITY_BUILDER)
                        .setIpPrices(IpPriceList.newBuilder()
                                .addIpPrice(IpConfigPrice.newBuilder().addPrices(Price.newBuilder()
                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                .setAmount(IP_PRICE_AMOUNT))))))
                .build();
        PriceTableKey.Builder priceTableKey = PriceTableKey.newBuilder().setServiceProviderId(123456L)
                .putProbeKeyMaterial("ENROLLMENT_NO", "123");
        ProbePriceData probePriceData = new ProbePriceData();
        probePriceData.riSpecPrices = Collections.emptyList();
        probePriceData.probeType = "AWS";
        probePriceData.priceTableKey = priceTableKey.build();
        probePriceData.priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable,
                cloudOidByLocalId, cloudEntitiesMap.getExtraneousIdLookUps(), SDKProbeType.AWS);
        ProbePriceData anotherprobePriceData = new ProbePriceData();
        anotherprobePriceData.priceTableKey = priceTableKey.build();
        anotherprobePriceData.priceTable = priceTableUploader.priceTableToCostPriceTable(sourcePriceTable,
                cloudOidByLocalId, cloudEntitiesMap.getExtraneousIdLookUps(), SDKProbeType.AWS);
        anotherprobePriceData.probeType = "AWS";
        anotherprobePriceData.riSpecPrices = Collections.emptyList();
        assertEquals("ProbePriceData are not equal", probePriceData, anotherprobePriceData);

        anotherprobePriceData.priceTableKey = priceTableKey.putProbeKeyMaterial("OFFER_ID", "456")
                .build();

        assertNotEquals("ProbePriceData were not supposed to be equal",
                probePriceData, anotherprobePriceData);
    }

    private PricingDTO.PriceTable createDefaultPriceTable() {
        return PricingDTO.PriceTable.newBuilder()
            .addOnDemandPriceTable(OnDemandPriceTableByRegionEntry.newBuilder()
                .setRelatedRegion(REGION_ENTITY_BUILDER))
                .setServiceProviderId("ServiceProvider")
            .build();
    }

    /**
     * Creating RI with a valid spec.
     *
     * @param region Region related to the RI.
     * @param entityType Common DTO entity type.
     * @param specId An Id assigned to the spec
     * @return RI spec builder.
     */
    private ReservedInstancePriceTableByRegionEntry.Builder createReservedInstancePriceTable(
        EntityDTO.Builder region, EntityType entityType, String specId) {
        return ReservedInstancePriceTableByRegionEntry.newBuilder()
            .setRelatedRegion(region)
            .addReservedInstancePriceMap(ReservedInstancePriceEntry.newBuilder()
                .setReservedInstanceSpec(ReservedInstanceSpec.newBuilder()
                    .setRegion(region)
                    .setTier(EntityDTO.newBuilder()
                        .setEntityType(entityType)
                        .setId(specId)
                        .build())
                    .build())
                .build());
    }

    /**
     * Creating RI with a invalid spec - missing a region.
     *
     * @param region Region related to the RI.
     * @param entityType Common DTO entity type.
     * @param specId An Id assigned to the spec
     * @return invalid RI spec builder.
     */
    private ReservedInstancePriceTableByRegionEntry.Builder createReservedInstancePriceTableNoRegion(
        EntityDTO.Builder region, EntityType entityType, String specId) {
        return ReservedInstancePriceTableByRegionEntry.newBuilder()
            .setRelatedRegion(region)
            .addReservedInstancePriceMap(ReservedInstancePriceEntry.newBuilder()
                .setReservedInstanceSpec(ReservedInstanceSpec.newBuilder()
                    .setTier(EntityDTO.newBuilder()
                        .setEntityType(entityType)
                        .setId(specId)
                        .build())
                    .build())
                .build());
    }

    /**
     * Creating RI with a invalid spec - missing a tier and a spec id.
     *
     * @param region Region related to the RI.
     * @return invalid RI spec builder.
     */
    private ReservedInstancePriceTableByRegionEntry.Builder createReservedInstancePriceTableNoTier(
        EntityDTO.Builder region) {
        return ReservedInstancePriceTableByRegionEntry.newBuilder()
            .setRelatedRegion(region)
            .addReservedInstancePriceMap(ReservedInstancePriceEntry.newBuilder()
                .setReservedInstanceSpec(ReservedInstanceSpec.newBuilder()
                    .setRegion(region)
                    .build())
                .build());
    }

    private static class TestPriceService extends PricingServiceImplBase {
    }
}
