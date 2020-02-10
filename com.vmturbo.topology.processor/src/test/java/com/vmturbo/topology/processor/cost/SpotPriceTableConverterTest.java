package com.vmturbo.topology.processor.cost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.PriceForGuestOsType;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.SpotPricesForTier;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.SpotPriceByRegionEntry;

/**
 * Unit tests for {@link SpotPriceTableConverter}.
 */
public class SpotPriceTableConverterTest {

    private static final String REGION_1_ID = "aws::DC::us-east-1";
    private static final String ZONE_1_ID = "aws::PM::us-east-1a";
    private static final String COMPUTE_TIER_1_ID = "aws::CT::t2.nano";
    private static final String REGION_2_ID = "aws::DC::us-west-1";
    private static final String ZONE_2_ID = "aws::PM::us-west-1b";
    private static final String COMPUTE_TIER_2_ID = "aws::CT::m4.large";
    private static final String REGION_NON_EXISTING_ID = "aws::DC::ca-central-1";
    private static final String ZONE_NON_EXISTING_ID = "aws::PM::ca-central-1a";
    private static final String COMPUTE_TIER_NON_EXISTING_ID = "aws::CT::i3.small";
    private static final Long REGION_1_OID = 1L;
    private static final Long ZONE_1_OID = 2L;
    private static final Long COMPUTE_TIER_1_OID = 3L;
    private static final Long REGION_2_OID = 4L;
    private static final Long ZONE_2_OID = 5L;
    private static final Long COMPUTE_TIER_2_OID = 6L;

    private static final Price PRICE_LINUX = Price.newBuilder().build();
    private static final Price PRICE_WINDOWS = Price.newBuilder().build();

    private static final Map<String, Long> CLOUD_ENTITIES_MAP = ImmutableMap.<String, Long>builder()
            .put(REGION_1_ID, REGION_1_OID)
            .put(ZONE_1_ID, ZONE_1_OID)
            .put(COMPUTE_TIER_1_ID, COMPUTE_TIER_1_OID)
            .put(REGION_2_ID, REGION_2_OID)
            .put(ZONE_2_ID, ZONE_2_OID)
            .put(COMPUTE_TIER_2_ID, COMPUTE_TIER_2_OID).build();

    /**
     * Test converting Spot prices from the same Region, Availability Zone and Compute Tier.
     */
    @Test
    public void testPopulateSpotPricesFromSingleLocation() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedZone(createZone(ZONE_1_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedZone(createZone(ZONE_1_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.WINDOWS)
                        .setPrice(PRICE_WINDOWS))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        // Expected map is:
        //     ZONE_1_OID -> COMPUTE_TIER_1_OID -> (LINUX_PRICE, WINDOWS_PRICE)
        assertEquals(1, spotPriceMap.size());
        assertTrue(spotPriceMap.containsKey(ZONE_1_OID));
        final Map<Long, SpotPricesForTier> spotPriceByTierOidMap = spotPriceMap
                .get(ZONE_1_OID).getSpotPricesByTierOidMap();
        assertEquals(1, spotPriceByTierOidMap.size());
        assertTrue(spotPriceByTierOidMap.containsKey(COMPUTE_TIER_1_OID));
        final List<PriceForGuestOsType> priceForGuestOsTypeList = spotPriceByTierOidMap
                .get(COMPUTE_TIER_1_OID).getPriceForGuestOsTypeList();
        assertEquals(2, priceForGuestOsTypeList.size());
        for (final PriceForGuestOsType priceForGuestOsType : priceForGuestOsTypeList) {
            final OSType osType = priceForGuestOsType.getGuestOsType();
            final Price price = priceForGuestOsType.getPrice();
            switch (osType) {
                case LINUX:
                    assertSame(PRICE_LINUX, price);
                    break;
                case WINDOWS:
                    assertSame(PRICE_WINDOWS, price);
                    break;
                default:
                    fail("Unexpected OS type: " + osType);
            }
        }
    }

    /**
     * Test converting Spot prices from different Regions, Availability Zones and Compute Tiers.
     */
    @Test
    public void testPopulateSpotPricesFromMultipleLocations() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedZone(createZone(ZONE_1_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_2_ID))
                        .setRelatedZone(createZone(ZONE_2_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_2_ID))
                        .setGuestOsType(OSType.WINDOWS)
                        .setPrice(PRICE_WINDOWS))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        // Expected map is:
        //     ZONE_1_OID -> COMPUTE_TIER_1_OID -> LINUX_PRICE
        //     ZONE_2_OID -> COMPUTE_TIER_2_OID -> WINDOWS_PRICE
        assertEquals(2, spotPriceMap.size());
        assertTrue(spotPriceMap.containsKey(ZONE_1_OID));
        final Map<Long, SpotPricesForTier> spotPriceByTierOidMap1 = spotPriceMap
                .get(ZONE_1_OID).getSpotPricesByTierOidMap();
        assertEquals(1, spotPriceByTierOidMap1.size());
        assertTrue(spotPriceByTierOidMap1.containsKey(COMPUTE_TIER_1_OID));
        final List<PriceForGuestOsType> priceForGuestOsTypeList1 = spotPriceByTierOidMap1
                .get(COMPUTE_TIER_1_OID).getPriceForGuestOsTypeList();
        assertEquals(1, priceForGuestOsTypeList1.size());
        final PriceForGuestOsType priceForGuestOsType1 = priceForGuestOsTypeList1.get(0);
        assertEquals(OSType.LINUX, priceForGuestOsType1.getGuestOsType());
        assertSame(PRICE_LINUX, priceForGuestOsType1.getPrice());

        assertTrue(spotPriceMap.containsKey(ZONE_2_OID));
        final Map<Long, SpotPricesForTier> spotPriceByTierOidMap2 = spotPriceMap
                .get(ZONE_2_OID).getSpotPricesByTierOidMap();
        assertEquals(1, spotPriceByTierOidMap2.size());
        assertTrue(spotPriceByTierOidMap2.containsKey(COMPUTE_TIER_2_OID));
        final List<PriceForGuestOsType> priceForGuestOsTypeList2 = spotPriceByTierOidMap2
                .get(COMPUTE_TIER_2_OID).getPriceForGuestOsTypeList();
        assertEquals(1, priceForGuestOsTypeList2.size());
        final PriceForGuestOsType priceForGuestOsType2 = priceForGuestOsTypeList2.get(0);
        assertEquals(OSType.WINDOWS, priceForGuestOsType2.getGuestOsType());
        assertSame(PRICE_WINDOWS, priceForGuestOsType2.getPrice());
    }

    /**
     * Test converting Spot prices from Region, with missing Availability Zone.
     */
    @Test
    public void testPopulateSpotPricesFromRegion() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        // Expected map is:
        //     REGION_1_OID -> COMPUTE_TIER_1_OID -> LINUX_PRICE
        assertEquals(1, spotPriceMap.size());
        assertTrue(spotPriceMap.containsKey(REGION_1_OID));
        final Map<Long, SpotPricesForTier> spotPriceByTierOidMap = spotPriceMap
                .get(REGION_1_OID).getSpotPricesByTierOidMap();
        assertEquals(1, spotPriceByTierOidMap.size());
        assertTrue(spotPriceByTierOidMap.containsKey(COMPUTE_TIER_1_OID));
        final List<PriceForGuestOsType> priceForGuestOsTypeList = spotPriceByTierOidMap
                .get(COMPUTE_TIER_1_OID).getPriceForGuestOsTypeList();
        assertEquals(1, priceForGuestOsTypeList.size());
        final PriceForGuestOsType priceForGuestOsType = priceForGuestOsTypeList.get(0);
        assertEquals(OSType.LINUX, priceForGuestOsType.getGuestOsType());
        assertSame(PRICE_LINUX, priceForGuestOsType.getPrice());
    }

    /**
     * Test converting Spot prices with duplicate entries.
     */
    @Test
    public void testPopulateSpotPricesWithDuplicates() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedZone(createZone(ZONE_1_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedZone(createZone(ZONE_1_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        // Expected map is:
        //     ZONE_1_OID -> COMPUTE_TIER_1_OID -> LINUX_PRICE
        assertEquals(1, spotPriceMap.size());
        assertTrue(spotPriceMap.containsKey(ZONE_1_OID));
        final Map<Long, SpotPricesForTier> spotPriceByTierOidMap = spotPriceMap
                .get(ZONE_1_OID).getSpotPricesByTierOidMap();
        assertEquals(1, spotPriceByTierOidMap.size());
        assertTrue(spotPriceByTierOidMap.containsKey(COMPUTE_TIER_1_OID));
        final List<PriceForGuestOsType> priceForGuestOsTypeList = spotPriceByTierOidMap
                .get(COMPUTE_TIER_1_OID).getPriceForGuestOsTypeList();
        assertEquals(1, priceForGuestOsTypeList.size());
        final PriceForGuestOsType priceForGuestOsType = priceForGuestOsTypeList.get(0);
        assertEquals(OSType.LINUX, priceForGuestOsType.getGuestOsType());
        assertSame(PRICE_LINUX, priceForGuestOsType.getPrice());
    }

    /**
     * Test converting Spot price with missing region.
     */
    @Test
    public void testPopulateSpotPricesWithMissingRegion() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedZone(createZone(ZONE_1_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        assertTrue(spotPriceMap.isEmpty());
    }

    /**
     * Test converting Spot price with missing region.
     */
    @Test
    public void testPopulateSpotPricesWithMissingComputeTier() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedZone(createZone(ZONE_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        assertTrue(spotPriceMap.isEmpty());
    }

    /**
     * Test converting Spot price with Region that doesn't exist in ID -> OID map.
     */
    @Test
    public void testPopulateSpotPricesWithNonExistingRegion() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_NON_EXISTING_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        assertTrue(spotPriceMap.isEmpty());
    }

    /**
     * Test converting Spot price with Availability Zone that doesn't exist in ID -> OID map.
     */
    @Test
    public void testPopulateSpotPricesWithNonExistingZone() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedZone(createZone(ZONE_NON_EXISTING_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_1_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        assertTrue(spotPriceMap.isEmpty());
    }

    /**
     * Test converting Spot price with Compute Tier that doesn't exist in ID -> OID map.
     */
    @Test
    public void testPopulateSpotPricesWithNonExistingComputeTier() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .addSpotPriceTable(SpotPriceByRegionEntry.newBuilder()
                        .setRelatedRegion(createRegion(REGION_1_ID))
                        .setRelatedZone(createZone(ZONE_1_ID))
                        .setRelatedComputeTier(createComputeTier(COMPUTE_TIER_NON_EXISTING_ID))
                        .setGuestOsType(OSType.LINUX)
                        .setPrice(PRICE_LINUX))
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        assertTrue(spotPriceMap.isEmpty());
    }

    /**
     * Test the case when price table doesn't contain any Spot prices.
     */
    @Test
    public void testPopulateSpotPricesFromEmptyTable() {
        // ARRANGE
        final PricingDTO.PriceTable sourcePriceTable = PricingDTO.PriceTable.newBuilder()
                .build();

        // ACT
        final Map<Long, SpotInstancePriceTable> spotPriceMap = convertSpotPrices(sourcePriceTable);

        // ASSERT
        assertTrue(spotPriceMap.isEmpty());
    }

    private Map<Long, SpotInstancePriceTable> convertSpotPrices(
            @Nonnull final PricingDTO.PriceTable sourcePriceTable) {
        return new SpotPriceTableConverter().convertSpotPrices(sourcePriceTable,
                CLOUD_ENTITIES_MAP);
    }

    private static EntityDTO createRegion(@Nonnull final String id) {
        return EntityDTO.newBuilder()
                .setEntityType(EntityType.REGION)
                .setId(id).build();
    }

    private static EntityDTO createZone(@Nonnull final String id) {
        return EntityDTO.newBuilder()
                .setEntityType(EntityType.AVAILABILITY_ZONE)
                .setId(id).build();
    }

    private static EntityDTO createComputeTier(@Nonnull final String id) {
        return EntityDTO.newBuilder()
                .setEntityType(EntityType.COMPUTE_TIER)
                .setId(id).build();
    }
}
