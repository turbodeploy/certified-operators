package com.vmturbo.cost.calculation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

public class CloudCostDataProviderTest {

    private CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);

    private static final long AWS_COMPUTE_TIER_ID = 95348;
    private static final long AZURE_COMPUTE_TIER_ID = 25387;
    private static final int NUM_OF_CORES = 4;

    private static final long AWS_BUSINESS_ACCOUNT_OID = 10;
    private static final long AZURE_BUSINESS_ACCOUNT_OID = 11;

    private static final long AWS_PRICE_TABLE_KEY_OID = 12;
    private static final long AZURE_PRICE_TABLE_KEY_OID = 13;

    private static final long REGION_ID = 8;

    // Base Price
    private static final double LINUX_PRICE = 0.096;

    // Price Adjustments
    private static final double WINDOWS_PRICE_ADJUSTMENT = 0.092;
    private static final double WINDOWS_BYOL_PRICE_ADJUSTMENT = LicensePriceTuple.NO_LICENSE_PRICE;
    private static final double WINDOWS_SQL_WEB_PRICE_ADJUSTMENT = 0.02;
    private static final double RHEL_PRICE_ADJUSTMENT = 0.06;

    // License Prices
    private static final double WINDOWS_SQL_WEB_LICENSE_PRICE = 0.005;
    private static final double RHEL_LICENSE_PRICE = 0.006;
    private static final double WINDOWS_SERVER_LICENSE_PRICE = 0.51;
    private static final double WINDOWS_BURSTABLE_LICENSE_PRICE = 1.02;


    private static final String LINUX = "Linux";
    private static final String RHEL = "RHEL";
    private static final String WINDOWS = "Windows";
    private static final String WINDOWS_BYOL = "Windows_Bring_your_own_license";
    private static final String WINDOWS_WITH_SQL_WEB = "Windows_SQL_Web";
    private static final boolean BURSTABLE_CPU = true;
    private static final boolean NOT_BURSTABLE_CPU = false;

    private static final PriceTable AWS_COMPUTE_PRICE_TABLE = PriceTable.newBuilder()
        .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
            .putComputePricesByTierId(AWS_COMPUTE_TIER_ID,
                createComputeTierPriceList(OSType.LINUX,
                    Arrays.asList(createComputeTierConfigPrice(OSType.LINUX, LINUX_PRICE),
                        createComputeTierConfigPrice(OSType.WINDOWS, WINDOWS_PRICE_ADJUSTMENT),
                        createComputeTierConfigPrice(OSType.WINDOWS_BYOL, WINDOWS_BYOL_PRICE_ADJUSTMENT),
                        createComputeTierConfigPrice(OSType.WINDOWS_WITH_SQL_WEB,
                            WINDOWS_SQL_WEB_PRICE_ADJUSTMENT),
                        createComputeTierConfigPrice(OSType.RHEL, RHEL_PRICE_ADJUSTMENT))))
            .build())
        .build();

    private static final PriceTable AZURE_COMPUTE_PRICE_TABLE = PriceTable.newBuilder()
        .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
            .putComputePricesByTierId(AZURE_COMPUTE_TIER_ID,
                createComputeTierPriceList(OSType.LINUX,
                    Arrays.asList(createComputeTierConfigPrice(OSType.LINUX, LINUX_PRICE),
                        createComputeTierConfigPrice(OSType.WINDOWS, WINDOWS_PRICE_ADJUSTMENT),
                        createComputeTierConfigPrice(OSType.WINDOWS_BYOL, WINDOWS_BYOL_PRICE_ADJUSTMENT))))
            .build())
        .addOnDemandLicensePrices(LicensePriceEntry.newBuilder()
            .setOsType(OSType.WINDOWS_WITH_SQL_WEB)
            .addLicensePrices(createLicensePrice(NUM_OF_CORES, WINDOWS_SQL_WEB_LICENSE_PRICE))
            .build())
            .addOnDemandLicensePrices(LicensePriceEntry.newBuilder()
                    .setOsType(OSType.RHEL)
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES * 2, RHEL_LICENSE_PRICE * 2))
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES, RHEL_LICENSE_PRICE))
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES * 4, RHEL_LICENSE_PRICE * 4))
                    .build())
            .addOnDemandLicensePrices(LicensePriceEntry.newBuilder()
                    .setOsType(OSType.WINDOWS)
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES, WINDOWS_BURSTABLE_LICENSE_PRICE ))
                    .setBurstableCPU(true)
                    .build())
            .addReservedLicensePrices(LicensePriceEntry.newBuilder()
                    .setOsType(OSType.WINDOWS)
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES * 2, WINDOWS_SERVER_LICENSE_PRICE * 2))
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES, WINDOWS_SERVER_LICENSE_PRICE))
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES * 3, WINDOWS_SERVER_LICENSE_PRICE * 3))
                    .build())
            .addReservedLicensePrices(LicensePriceEntry.newBuilder()
                    .setOsType(OSType.WINDOWS)
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES * 4, WINDOWS_SERVER_LICENSE_PRICE * 4))
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES * 5, WINDOWS_SERVER_LICENSE_PRICE * 5))
                    .build())
            .addReservedLicensePrices(LicensePriceEntry.newBuilder()
                    .setOsType(OSType.WINDOWS)
                    .setBurstableCPU(true)
                    .addLicensePrices(createLicensePrice(NUM_OF_CORES, WINDOWS_BURSTABLE_LICENSE_PRICE))
                    .build())
        .build();
    private final DiscountApplicator<TopologyEntityDTO> discountApplicator =
            mock(DiscountApplicator.class);

    private final AccountPricingData<TopologyEntityDTO> awsAccountPricingData =
            new AccountPricingData<>( discountApplicator, AWS_COMPUTE_PRICE_TABLE,
                    AWS_BUSINESS_ACCOUNT_OID);

    private final AccountPricingData<TopologyEntityDTO> azureAccountPricingData =
            new AccountPricingData<>( discountApplicator, AZURE_COMPUTE_PRICE_TABLE,
                    AZURE_BUSINESS_ACCOUNT_OID);

    private Map<Long, AccountPricingData<TopologyEntityDTO>> awsAccountPricingDatByBaMap
            = new HashMap<>();

    private Map<Long, AccountPricingData<TopologyEntityDTO>> azureAccountPricingDatByBaMap
            = new HashMap<>();

    private CloudCostData<TopologyEntityDTO> cloudCostDataAWS =
            new CloudCostData<>(Collections.emptyMap(), Collections.emptyMap(),
    Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), awsAccountPricingDatByBaMap);

    private CloudCostData<TopologyEntityDTO> cloudCostDataAzure =
            new CloudCostData<>(Collections.emptyMap(), Collections.emptyMap(),
    Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), azureAccountPricingDatByBaMap);

    /**
     * Create a Price instance
     * @param amount number of dollars
     * @return A price object with the given fields
     */
    private static Price createPrice(double amount) {
        return Price.newBuilder()
            .setUnit(Unit.HOURS)
            .setPriceAmount(CurrencyAmount.newBuilder()
                .setAmount(amount)
                .build())
            .build();
    }

    /**
     * Create a LicensePrice instance
     * @param numCores the number of cores for the required LicensePrice
     * @param price the price of that license for the given number of cores
     * @return A LicensePrice object with the given fields
     */
    private static LicensePrice createLicensePrice(int numCores, double price) {
        return LicensePrice.newBuilder()
            .setNumberOfCores(numCores)
            .setPrice(createPrice(price))
            .build();
    }

    /**
     * Create a ComputeTierConfigPrice instance
     * @param os the OS for which we want to create a ComputeTierConfigPrice
     * @param price the dollar/hour price
     * @return A ComputeTierConfigPrice object with the given fields
     */
    private static ComputeTierConfigPrice createComputeTierConfigPrice(OSType os, double price) {
        return ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(os)
            .addPrices(createPrice(price))
            .build();
    }

    /**
     * Create a ComputeTierPriceList instance
     * @param baseOS the base OS for the price list
     * @param prices the prices for the price list
     * @return A ComputeTierPriceList object with the given prices
     */
    private static ComputeTierPriceList createComputeTierPriceList(OSType baseOS, List<ComputeTierConfigPrice> prices) {
        ComputeTierPriceList.Builder computePriceList =  ComputeTierPriceList.newBuilder();

        for (ComputeTierConfigPrice computePrice : prices) {
            if (computePrice.getGuestOsType() == baseOS) {
                computePriceList.setBasePrice(computePrice);
            } else {
                computePriceList.addPerConfigurationPriceAdjustments(computePrice);
            }
        }

        return computePriceList.build();
    }

    /**
     * Create a CommoditySoldDTO instance
     * @param os the OS for which we want to create a license commodity
     * @return A CommoditySoldDTO object for the given OS
     */
    private static CommoditySoldDTO createCommoditySoldDTO(String os) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .setKey(os).build()).build();
    }

    /**
     * Get the relevant price list from the price table.
     * @param cloudCostData the {@link CloudCostData} from which to take the price table.
     * @param tierID the tier for which to get the compute prices of.
     * @param baOid the business account oid.
     *
     * @return a list of all compute prices for this tier.
     */
    private ComputeTierPriceList getComputePriceList(CloudCostData<TopologyEntityDTO> cloudCostData,
                                                     long tierID, long baOid) {
        return cloudCostData.getAccountPricingData(baOid).get().getPriceTable().getOnDemandPriceByRegionIdMap().get(REGION_ID)
                .getComputePricesByTierIdMap().get(tierID);
    }

    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.REGION_VALUE)
        .setOid(REGION_ID)
        .build();

    private static final TopologyEntityDTO AWS_COMPUTE_TIER = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(AWS_COMPUTE_TIER_ID)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(ComputeTierInfo.newBuilder()
            .setNumCoupons(1)
            .setNumCores(NUM_OF_CORES)
            .build())
            .build())
        .addCommoditySoldList(createCommoditySoldDTO(LINUX))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS_BYOL))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS_WITH_SQL_WEB))
        .addCommoditySoldList(createCommoditySoldDTO(RHEL))
        .build();

    private static final TopologyEntityDTO AZURE_COMPUTE_TIER = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(AZURE_COMPUTE_TIER_ID)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(ComputeTierInfo.newBuilder()
            .setNumCoupons(1)
            .setNumCores(NUM_OF_CORES)
            .build())
            .build())
        .addCommoditySoldList(createCommoditySoldDTO(LINUX))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS_BYOL))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS_WITH_SQL_WEB))
        .addCommoditySoldList(createCommoditySoldDTO(RHEL))
        .build();

    @Before
    public void setup() {
        when(topology.getEntity(REGION_ID)).thenReturn(Optional.of(REGION));
        when(topology.getEntity(AWS_COMPUTE_TIER_ID)).thenReturn(Optional.of(AWS_COMPUTE_TIER));
        when(topology.getEntity(AZURE_COMPUTE_TIER_ID)).thenReturn(Optional.of(AZURE_COMPUTE_TIER));
        awsAccountPricingDatByBaMap.put(AWS_BUSINESS_ACCOUNT_OID, awsAccountPricingData);
        azureAccountPricingDatByBaMap.put(AZURE_BUSINESS_ACCOUNT_OID, azureAccountPricingData);
    }

    /**
     * Test that for Linux and Windows_BYOL, there is no license price.
     */
    @Test
    public void testNoLicensePrice() {
        LicensePriceTuple licensePriceTuple = cloudCostDataAWS.getAccountPricingData(AWS_BUSINESS_ACCOUNT_OID).get().getLicensePrice(OSType.LINUX,
            NUM_OF_CORES, getComputePriceList(cloudCostDataAWS, AWS_COMPUTE_TIER_ID, AWS_BUSINESS_ACCOUNT_OID), NOT_BURSTABLE_CPU );
        assertThat(licensePriceTuple.getImplicitOnDemandLicensePrice(),
            equalTo(LicensePriceTuple.NO_LICENSE_PRICE));
        assertThat(licensePriceTuple.getExplicitOnDemandLicensePrice(),
            equalTo(LicensePriceTuple.NO_LICENSE_PRICE));
        assertThat(licensePriceTuple.getReservedInstanceLicensePrice(), equalTo(LicensePriceTuple.NO_LICENSE_PRICE));

        licensePriceTuple = cloudCostDataAzure.getAccountPricingData(AZURE_BUSINESS_ACCOUNT_OID).get().getLicensePrice(OSType.WINDOWS_BYOL,
            NUM_OF_CORES, getComputePriceList(cloudCostDataAzure, AZURE_COMPUTE_TIER_ID, AZURE_BUSINESS_ACCOUNT_OID), NOT_BURSTABLE_CPU );
        assertThat(licensePriceTuple.getImplicitOnDemandLicensePrice(),
            equalTo(LicensePriceTuple.NO_LICENSE_PRICE));
        assertThat(licensePriceTuple.getExplicitOnDemandLicensePrice(),
            equalTo(LicensePriceTuple.NO_LICENSE_PRICE));
        assertThat(licensePriceTuple.getReservedInstanceLicensePrice(), equalTo(LicensePriceTuple.NO_LICENSE_PRICE));
    }

    /**
     * Test cases in which we expect only an implicit price (calculated license price).
     */
    @Test
    public void testOnlyImplicitLicensePrice() {
        LicensePriceTuple licensePriceTuple = cloudCostDataAWS.getAccountPricingData(AWS_BUSINESS_ACCOUNT_OID).get().getLicensePrice(
            OSType.WINDOWS_WITH_SQL_WEB, NUM_OF_CORES,
            getComputePriceList(cloudCostDataAWS, AWS_COMPUTE_TIER_ID, AWS_BUSINESS_ACCOUNT_OID), NOT_BURSTABLE_CPU );
        assertThat(licensePriceTuple.getImplicitOnDemandLicensePrice(),
            equalTo(WINDOWS_SQL_WEB_PRICE_ADJUSTMENT));
        assertThat(licensePriceTuple.getExplicitOnDemandLicensePrice(),
            equalTo(LicensePriceTuple.NO_LICENSE_PRICE));

        licensePriceTuple = cloudCostDataAzure.getAccountPricingData(AZURE_BUSINESS_ACCOUNT_OID).get().getLicensePrice(OSType.WINDOWS, NUM_OF_CORES,
            getComputePriceList(cloudCostDataAzure, AZURE_COMPUTE_TIER_ID, AZURE_BUSINESS_ACCOUNT_OID), NOT_BURSTABLE_CPU );
        assertThat(licensePriceTuple.getImplicitOnDemandLicensePrice(), equalTo(WINDOWS_PRICE_ADJUSTMENT));
        assertThat(licensePriceTuple.getExplicitOnDemandLicensePrice(),
            equalTo(LicensePriceTuple.NO_LICENSE_PRICE));

        licensePriceTuple = cloudCostDataAzure.getAccountPricingData(AZURE_BUSINESS_ACCOUNT_OID).get().getLicensePrice(OSType.WINDOWS, NUM_OF_CORES,
                        getComputePriceList(cloudCostDataAzure, AZURE_COMPUTE_TIER_ID, AZURE_BUSINESS_ACCOUNT_OID), NOT_BURSTABLE_CPU );
        assertThat(licensePriceTuple.getImplicitOnDemandLicensePrice(), equalTo(WINDOWS_PRICE_ADJUSTMENT));
        assertThat(licensePriceTuple.getReservedInstanceLicensePrice(), equalTo(WINDOWS_SERVER_LICENSE_PRICE));
        assertThat(licensePriceTuple.getExplicitOnDemandLicensePrice(), equalTo(LicensePriceTuple.NO_LICENSE_PRICE));
    }

    /**
     * Test cases in which we expect only an explicit price (catalog price)
     */
    @Test
    public void testOnlyExplicitLicensePrice() {
        LicensePriceTuple licensePriceTuple = cloudCostDataAzure.getAccountPricingData(AZURE_BUSINESS_ACCOUNT_OID).get().getLicensePrice(OSType.RHEL,
                NUM_OF_CORES, getComputePriceList(cloudCostDataAzure, AZURE_COMPUTE_TIER_ID, AZURE_BUSINESS_ACCOUNT_OID), NOT_BURSTABLE_CPU );
        assertThat(licensePriceTuple.getImplicitOnDemandLicensePrice(),
            equalTo(LicensePriceTuple.NO_LICENSE_PRICE));
        assertThat(licensePriceTuple.getExplicitOnDemandLicensePrice(), equalTo(RHEL_LICENSE_PRICE));
    }


    /**
     * Test cases in which we expect only an explicit burstable price (catalog price).
     */
    @Test
    public void testOnlyExplicitLicensePriceWithBurstable() {
        LicensePriceTuple licensePriceTuple = cloudCostDataAzure.getAccountPricingData(AZURE_BUSINESS_ACCOUNT_OID).get()
                .getLicensePrice(OSType.WINDOWS, NUM_OF_CORES,
                    getComputePriceList(cloudCostDataAzure, AZURE_COMPUTE_TIER_ID, AZURE_BUSINESS_ACCOUNT_OID),
                        BURSTABLE_CPU);
        assertThat(licensePriceTuple.getExplicitOnDemandLicensePrice(), equalTo(WINDOWS_BURSTABLE_LICENSE_PRICE));
    }

    /**
     * Test cases in which we expect both the implicit price and the explicit price
     */
    @Test
    public void testImplicitAndExplicitLicensePrice() {
        LicensePriceTuple licensePriceTuple = cloudCostDataAzure.getAccountPricingData(AZURE_BUSINESS_ACCOUNT_OID).get().getLicensePrice(
            OSType.WINDOWS_WITH_SQL_WEB, NUM_OF_CORES,
            getComputePriceList(cloudCostDataAzure, AZURE_COMPUTE_TIER_ID, AZURE_BUSINESS_ACCOUNT_OID), NOT_BURSTABLE_CPU );
        assertThat(licensePriceTuple.getImplicitOnDemandLicensePrice(), equalTo(WINDOWS_PRICE_ADJUSTMENT));
        assertThat(licensePriceTuple.getExplicitOnDemandLicensePrice(),
            equalTo(WINDOWS_SQL_WEB_LICENSE_PRICE));
    }
}
