package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.math.BigDecimal;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

/**
 * This class provides constants for other tests.
 * Create
 *   historical data indices: multiple hours and days
 *   accounts: two master accounts with different number of subaccounts
 *   locations: two regions with different number of availability zones
 *   compute tiers: two families with different number of instance types
 *
 */
public class ReservedInstanceAnalyzerConstantsTest {

    private ReservedInstanceAnalyzerConstantsTest() {}

    // Byte hour, Byte day, Long accountId, Long computeTierId, Long zoneId, Byte platform, Byte tenancy, BigDecimal allocation, BigDecimal consumption)
    // hour
    static final Byte HOUR1 = new Byte((byte)1);
    static final Byte HOUR2 = new Byte((byte)2);
    static final Byte HOUR3 = new Byte((byte)3);
    // day
    static final Byte DAY1 = new Byte((byte)1);
    static final Byte DAY4 = new Byte((byte)4);

    // billing family 1
    static final long MASTER_ACCOUNT_1_OID = 300L;
    static final long BUSINESS_ACCOUNT_11_OID = 301L;
    static final long BUSINESS_ACCOUNT_12_OID = 302L;
    // billing family 2
    static final long MASTER_ACCOUNT_2_OID = 310L;
    static final long BUSINESS_ACCOUNT_21_OID = 311L;

    static final long AWS_TARGET_ID = 77777L;
    static final Origin AWS_ORIGIN = Origin.newBuilder()
        .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
            .putDiscoveredTargetData(AWS_TARGET_ID, PerTargetEntityInformation.getDefaultInstance()))
        .build();

    /*
     * location
     */
    static final long REGION_AWS_OHIO_OID = 500L;
    static final long ZONE_AWS_OHIO_1_OID = 501L;
    static final long ZONE_AWS_OHIO_2_OID = 502L;
    static final long REGION_AWS_OREGON_OID = 5100L;
    static final long ZONE_AWS_OREGON_1_OID = 511L;
    static final long ZONE_AWS_OREGON_2_OID = 512L;
    static final long ZONE_UNDEFINED_OID = 0L;

    static final TopologyEntityDTO ZONE_AWS_OHIO_1 = TopologyEntityDTO.newBuilder()
        .setOid(ZONE_AWS_OHIO_1_OID)
        .setDisplayName("aws_ohio_zone_1")
        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();

    static final TopologyEntityDTO ZONE_AWS_OHIO_2 = TopologyEntityDTO.newBuilder()
        .setOid(ZONE_AWS_OHIO_2_OID)
        .setDisplayName("aws_ohio_zone_2")
        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();
    static final TopologyEntityDTO REGION_AWS_OHIO = TopologyEntityDTO.newBuilder()
        .setOid(REGION_AWS_OHIO_OID)
        .setDisplayName("aws-ohio")
        .setEntityType(EntityType.REGION_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(ZONE_AWS_OHIO_1.getEntityType())
            .setConnectedEntityId(ZONE_AWS_OHIO_1_OID)
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(ZONE_AWS_OHIO_2.getEntityType())
            .setConnectedEntityId(ZONE_AWS_OHIO_2_OID)
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .build();

    static final TopologyEntityDTO ZONE_AWS_OREGON_1 = TopologyEntityDTO.newBuilder()
        .setOid(ZONE_AWS_OREGON_1_OID)
        .setDisplayName("aws-oregon_zone_1")
        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();
    static final TopologyEntityDTO ZONE_AWS_OREGON_2 = TopologyEntityDTO.newBuilder()
        .setOid(ZONE_AWS_OREGON_2_OID)
        .setDisplayName("aws-oregon_zone_2")
        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .build();
    static final TopologyEntityDTO REGION_AWS_OREGON = TopologyEntityDTO.newBuilder()
        .setOid(REGION_AWS_OREGON_OID)
        .setDisplayName("aws-oregon")
        .setEntityType(EntityType.REGION_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(ZONE_AWS_OREGON_1.getEntityType())
            .setConnectedEntityId(ZONE_AWS_OREGON_1.getOid())
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(ZONE_AWS_OREGON_2.getEntityType())
            .setConnectedEntityId(ZONE_AWS_OREGON_2.getOid())
            .setConnectionType(ConnectionType.OWNS_CONNECTION))
        .build();

    /*
     * compute tiers
     */
    static final long COMPUTE_TIER_M5_LARGE_OID = 4204;
    static final String COMPUTE_TIER_M5_FAMILY = "m5";
    static final long COMPUTE_TIER_T2_NANO_OID = 4110;
    static final long COMPUTE_TIER_T2_MICRO_OID = 4111;
    static final String COMPUTE_TIER_T2_FAMILY = "t2";
    static final int NUM_COUPONS = 1;

    static final ComputeTierInfo t2ComputeTierInfo = ComputeTierInfo.newBuilder()
            .setFamily(COMPUTE_TIER_T2_FAMILY)
            .setNumCoupons(NUM_COUPONS)
            .build();
    static final TypeSpecificInfo t2TypeSpecificInfo = TypeSpecificInfo.newBuilder()
            .setComputeTier(t2ComputeTierInfo)
            .build();
    static final ComputeTierInfo m5ComputeTierInfo = ComputeTierInfo.newBuilder()
            .setFamily(COMPUTE_TIER_M5_FAMILY)
            .build();
    static final TypeSpecificInfo m5TypeSpecificInfo = TypeSpecificInfo.newBuilder()
            .setComputeTier(m5ComputeTierInfo)
            .build();
    static final TopologyEntityDTO COMPUTE_TIER_M5_LARGE = TopologyEntityDTO.newBuilder()
        .setOid(COMPUTE_TIER_M5_LARGE_OID)
        .setDisplayName("m5.large")
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OHIO.getEntityType())
            .setConnectedEntityId(REGION_AWS_OHIO.getOid())
            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
        .setTypeSpecificInfo(m5TypeSpecificInfo)
        .build();

    static final TopologyEntityDTO COMPUTE_TIER_T2_NANO = TopologyEntityDTO.newBuilder()
        .setOid(COMPUTE_TIER_T2_NANO_OID)
        .setDisplayName("t2.nano")
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OHIO.getEntityType())
            .setConnectedEntityId(REGION_AWS_OHIO.getOid())
            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OREGON.getEntityType())
            .setConnectedEntityId(REGION_AWS_OREGON.getOid())
            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
        .setTypeSpecificInfo(t2TypeSpecificInfo)
        .build();

    static final TopologyEntityDTO COMPUTE_TIER_T2_MICRO = TopologyEntityDTO.newBuilder()
        .setOid(COMPUTE_TIER_T2_MICRO_OID)
        .setDisplayName("t2.micro")
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOrigin(AWS_ORIGIN)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OHIO.getEntityType())
            .setConnectedEntityId(REGION_AWS_OHIO.getOid())
            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(REGION_AWS_OREGON.getEntityType())
            .setConnectedEntityId(REGION_AWS_OREGON.getOid())
            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
        .setTypeSpecificInfo(t2TypeSpecificInfo)
        .build();
    static final BigDecimal VALUE_ALLOCATION = new BigDecimal(10);
    static final BigDecimal VALUE_CONSUMPTION = new BigDecimal(20);

    /*
     * ReservedInstanceType and purchase constratints
     */
    static final ReservedInstanceType RESERVED_INSTANCE_TYPE_1 =  ReservedInstanceType.newBuilder()
        .setOfferingClass(OfferingClass.STANDARD)
        .setPaymentOption(PaymentOption.NO_UPFRONT)
        .setTermYears(1).build();
    static final ReservedInstancePurchaseConstraints PURCHASE_CONSTRAINTS_FROM_TYPE_1 =
        new ReservedInstancePurchaseConstraints(RESERVED_INSTANCE_TYPE_1);
    static final ReservedInstancePurchaseConstraints PURCHASE_CONSTRAINTS_1 =
        new ReservedInstancePurchaseConstraints(OfferingClass.STANDARD, 1, PaymentOption.NO_UPFRONT);
    static final ReservedInstanceType RESERVED_INSTANCE_TYPE_2 =  ReservedInstanceType.newBuilder()
        .setOfferingClass(OfferingClass.CONVERTIBLE)
        .setPaymentOption(PaymentOption.ALL_UPFRONT)
        .setTermYears(3).build();
    static final ReservedInstancePurchaseConstraints PURCHASE_CONSTRAINTS_FROM_TYPE_2 =
        new ReservedInstancePurchaseConstraints(RESERVED_INSTANCE_TYPE_2);
    static final ReservedInstancePurchaseConstraints PURCHASE_CONSTRAINTS_2 =
        new ReservedInstancePurchaseConstraints(OfferingClass.CONVERTIBLE, 3, PaymentOption.ALL_UPFRONT);

    /*
     * ReservedInstanceRegionalContext
     */
    static final TopologyEntityDTO regionOhio = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.REGION_VALUE)
                        .setOid(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID)
                        .setDisplayName("ohio region")
                        .build();
    static final TopologyEntityDTO regionOregon = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.REGION_VALUE)
                        .setOid(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID)
                        .setDisplayName("oregon region")
                        .build();
    static final ReservedInstanceRegionalContext REGIONAL_CONTEXT_1 =
        new ReservedInstanceRegionalContext(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
            OSType.LINUX,
            Tenancy.DEFAULT,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE,
            regionOhio);
    static final ReservedInstanceRegionalContext REGIONAL_CONTEXT_2 =
        new ReservedInstanceRegionalContext(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID,
            OSType.WINDOWS,
            Tenancy.DEDICATED,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO,
            regionOregon);
    static final ReservedInstanceRegionalContext REGIONAL_CONTEXT_4 =
        new ReservedInstanceRegionalContext(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID,
            OSType.RHEL,
            Tenancy.DEFAULT,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE,
            regionOregon);
    static final ReservedInstanceRegionalContext REGIONAL_CONTEXT_5 =
        new ReservedInstanceRegionalContext(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID,
            OSType.SUSE,
            Tenancy.DEFAULT,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO,
            regionOhio);

    /*
     * ReservedInstanceSpecInfo
     */
    static final boolean SIZE_FLEXIBLE_FALSE = false;
    static final boolean SIZE_FLEXIBLE_TRUE = true;
    static final ReservedInstanceSpecInfo RI_SPEC_INFO_1 = ReservedInstanceSpecInfo.newBuilder()
        .setOs(OSType.LINUX)
        .setTenancy(Tenancy.DEFAULT)
        .setTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID)
        .setRegionId(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID)
        .setType(RESERVED_INSTANCE_TYPE_1)
        .setSizeFlexible(SIZE_FLEXIBLE_TRUE)
        .build();
    static final ReservedInstanceSpecInfo RI_SPEC_INFO_2 = ReservedInstanceSpecInfo.newBuilder()
        .setOs(OSType.LINUX)
        .setTenancy(Tenancy.DEFAULT)
        .setTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID)
        .setRegionId(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID)
        .setType(RESERVED_INSTANCE_TYPE_2)
        .setSizeFlexible(SIZE_FLEXIBLE_TRUE)
        .build();
    static final ReservedInstanceSpecInfo RI_SPEC_INFO_3 = ReservedInstanceSpecInfo.newBuilder()
        .setOs(OSType.WINDOWS)
        .setTenancy(Tenancy.DEDICATED)
        .setTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO_OID)
        .setRegionId(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID)
        .setType(RESERVED_INSTANCE_TYPE_1)
        .setSizeFlexible(SIZE_FLEXIBLE_FALSE)
        .build();
    static final ReservedInstanceSpecInfo RI_SPEC_INFO_4 = ReservedInstanceSpecInfo.newBuilder()
        .setOs(OSType.WINDOWS)
        .setTenancy(Tenancy.DEDICATED)
        .setTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO_OID)
        .setRegionId(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID)
        .setType(RESERVED_INSTANCE_TYPE_2)
        .setSizeFlexible(SIZE_FLEXIBLE_FALSE)
        .build();


    static final ReservedInstanceSpec RI_SPEC_1 = ReservedInstanceSpec.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_1)
        .setReservedInstanceSpecInfo(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1).build();
    static final ReservedInstanceSpec RI_SPEC_2 = ReservedInstanceSpec.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_2)
        .setReservedInstanceSpecInfo(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_2).build();
    static final ReservedInstanceSpec RI_SPEC_3 = ReservedInstanceSpec.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_3)
        .setReservedInstanceSpecInfo(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_3).build();
    static final ReservedInstanceSpec RI_SPEC_4 = ReservedInstanceSpec.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_4)
        .setReservedInstanceSpecInfo(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_4).build();

    /*
     * ReservedInstanceSpec IDs
     */
    static final long RESERVED_INSTANCE_SPEC_ID_1 = 100001L;
    static final long RESERVED_INSTANCE_SPEC_ID_2 = 100002L;
    static final long RESERVED_INSTANCE_SPEC_ID_3 = 100003L;
    static final long RESERVED_INSTANCE_SPEC_ID_4 = 100004L;

    /*
     * ReservedInstanceBoughtInfo
     */
    static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_1 =
        ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID)
            .setAvailabilityZoneId(ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_1_OID)
            .setReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_1)
            .build();
    static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_2 =
        ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID)
            .setAvailabilityZoneId(ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_2_OID)
            .setReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_2)
            .build();
    static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_3 =
        ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID)
            .setAvailabilityZoneId(ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OREGON_1_OID)
            .setReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_3)
            .build();
    static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_4 =
        ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID)
            .setAvailabilityZoneId(ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OREGON_1_OID)
            .setReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_4)
            .build();

    static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_1 =
        ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID)
            .setReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_1)
            .build();
    static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_2 =
        ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID)
            .setReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_2)
            .build();
    static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_3 =
        ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID)
            .setReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_3)
            .build();
    static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_4 =
        ReservedInstanceBoughtInfo.newBuilder()
            .setBusinessAccountId(ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID)
            .setReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_4)
            .build();

    /*
     * ReservedInstanceBought IDs
     */
    static final long RESERVED_INSTANCE_BOUGHT_ID_1 = 200001;
    static final long RESERVED_INSTANCE_BOUGHT_ID_2 = 200002;
    static final long RESERVED_INSTANCE_BOUGHT_ID_3 = 200003;
    static final long RESERVED_INSTANCE_BOUGHT_ID_4 = 200004;
    static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_1 = ReservedInstanceBought.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_ID_1)
        .setReservedInstanceBoughtInfo(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_1)
        .build();
    static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_2 = ReservedInstanceBought.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_ID_2)
        .setReservedInstanceBoughtInfo(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_2)
        .build();
    static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_3 = ReservedInstanceBought.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_ID_3)
        .setReservedInstanceBoughtInfo(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_3)
        .build();
    static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_4 = ReservedInstanceBought.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_ID_4)
        .setReservedInstanceBoughtInfo(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_4)
        .build();

    static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_REGIONAL_1 = ReservedInstanceBought.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_ID_1)
        .setReservedInstanceBoughtInfo(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_1)
        .build();
    static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_REGIONAL_2 = ReservedInstanceBought.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_ID_2)
        .setReservedInstanceBoughtInfo(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_2)
        .build();
    static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_REGIONAL_3 = ReservedInstanceBought.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_ID_3)
        .setReservedInstanceBoughtInfo(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_3)
        .build();
    static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_REGIONAL_4 = ReservedInstanceBought.newBuilder()
        .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_ID_4)
        .setReservedInstanceBoughtInfo(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_4)
        .build();

    /*
     * prices
     */
    static final Unit UNIT_HOURS = Unit.HOURS;
    static final Unit UNIT_TOTAL = Unit.TOTAL;
    static final Unit UNIT_MONTHS = Unit.MONTH;

    static final CurrencyAmount HOURLY_CURRENCY_AMOUNT_1 = CurrencyAmount.newBuilder().setAmount(1D).build();
    static final CurrencyAmount HOURLY_CURRENCY_AMOUNT_2 = CurrencyAmount.newBuilder().setAmount(2D).build();
    static final CurrencyAmount HOURLY_CURRENCY_AMOUNT_3 = CurrencyAmount.newBuilder().setAmount(3D).build();
    static final CurrencyAmount HOURLY_CURRENCY_AMOUNT_4 = CurrencyAmount.newBuilder().setAmount(4D).build();

    static final CurrencyAmount MONTHLY_CURRENCY_AMOUNT_1 = CurrencyAmount.newBuilder().setAmount(30d).build();
    static final CurrencyAmount MONTHLY_CURRENCY_AMOUNT_2 = CurrencyAmount.newBuilder().setAmount(60d).build();

    static final CurrencyAmount TOTAL_CURRENCY_AMOUNT_1 = CurrencyAmount.newBuilder().setAmount(100d).build();
    static final CurrencyAmount TOTAL_CURRENCY_AMOUNT_2 = CurrencyAmount.newBuilder().setAmount(200d).build();

    static final Price PRICE_HOURLY_1 = Price.newBuilder()
        .setUnit(UNIT_HOURS)
        .setPriceAmount(HOURLY_CURRENCY_AMOUNT_1).build();
    static final Price PRICE_HOURLY_2 = Price.newBuilder()
        .setUnit(UNIT_HOURS)
        .setPriceAmount(HOURLY_CURRENCY_AMOUNT_2).build();
    static final Price PRICE_HOURLY_3 = Price.newBuilder()
        .setUnit(UNIT_HOURS)
        .setPriceAmount(HOURLY_CURRENCY_AMOUNT_3).build();
    static final Price PRICE_HOURLY_4 = Price.newBuilder()
        .setUnit(UNIT_HOURS)
        .setPriceAmount(HOURLY_CURRENCY_AMOUNT_4).build();
    static final Price PRICE_MONTH_1 = Price.newBuilder()
        .setUnit(UNIT_MONTHS)
        .setPriceAmount(MONTHLY_CURRENCY_AMOUNT_1).build();

    static final Price PRICE_MONTH_2 = Price.newBuilder()
        .setUnit(UNIT_MONTHS)
        .setPriceAmount(MONTHLY_CURRENCY_AMOUNT_2).build();
    static final Price PRICE_UPFRONT_1 = Price.newBuilder()
        .setUnit(UNIT_TOTAL)
        .setPriceAmount(TOTAL_CURRENCY_AMOUNT_1).build();
    static final Price PRICE_UPFRONT_2 = Price.newBuilder()
        .setUnit(UNIT_TOTAL)
        .setPriceAmount(TOTAL_CURRENCY_AMOUNT_2).build();
}
