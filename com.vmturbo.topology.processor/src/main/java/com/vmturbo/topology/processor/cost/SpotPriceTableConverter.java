package com.vmturbo.topology.processor.cost;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.PriceForGuestOsType;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.SpotPricesForTier;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.SpotPriceByRegionEntry;

/**
 * {@code SpotPriceTableConverter} converts Spot price table received from Cost discovery to the
 * map of {@link SpotInstancePriceTable}.
 */
public class SpotPriceTableConverter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Convert Spot price table to the map of {@link SpotInstancePriceTable}.
     *
     * @param sourcePriceTable Source price table received from Mediation Cost discovery.
     * @param cloudEntitiesMap Mapping from vendor ID to OID.
     * @return Map of Zone/Region OID -> {@link SpotInstancePriceTable}.
     */
    public Map<Long, SpotInstancePriceTable> convertSpotPrices(
            @Nonnull final PricingDTO.PriceTable sourcePriceTable,
            @Nonnull final Map<String, Long> cloudEntitiesMap) {
        return convertToSpotInstancePriceTableMap(
                parsePriceTable(sourcePriceTable, cloudEntitiesMap));
    }

    /**
     * Convert source price table from Cost discovery to the map.
     *
     * @param sourcePriceTable Source price table received from Mediation Cost discovery.
     * @param cloudEntitiesMap Mapping from vendor ID to OID.
     * @return Map: Zone/Region ID -> Compute Tier ID -> guest OS type -> Price.
     */
    private Map<Long, Map<Long, Map<OSType, Price>>> parsePriceTable(
            @Nonnull final PricingDTO.PriceTable sourcePriceTable,
            @Nonnull final Map<String, Long> cloudEntitiesMap) {
        // Zone/Region ID -> Compute Tier ID -> guest OS type -> Price
        final Map<Long, Map<Long, Map<OSType, Price>>> spotPriceMap = new HashMap<>();
        for (final SpotPriceByRegionEntry spotPriceByRegionEntry : sourcePriceTable.getSpotPriceTableList()) {
            if (!spotPriceByRegionEntry.hasRelatedRegion()) {
                logger.error("Spot price table reader: region is mandatory for Spot prices: {}",
                        spotPriceByRegionEntry.toString());
                continue;
            }
            final String regionId = spotPriceByRegionEntry.getRelatedRegion().getId();

            final Long zoneOrRegionOid;
            if (spotPriceByRegionEntry.hasRelatedZone()) {
                final String zoneId = spotPriceByRegionEntry.getRelatedZone().getId();
                zoneOrRegionOid = cloudEntitiesMap.get(zoneId);
                if (zoneOrRegionOid == null) {
                    // This is "debug" rather than "warning" because some zones may not be available
                    // (e.g. zones from opt-in regions).
                    logger.debug("Spot price table reader: OID not found for zone ID {}", zoneId);
                    continue;
                }
            } else {
                zoneOrRegionOid = cloudEntitiesMap.get(regionId);
                if (zoneOrRegionOid == null) {
                    // This is "debug" rather than "warning" because some regions may not be available
                    // (e.g. opt-in regions).
                    logger.debug("Spot price table reader: OID not found for region ID {}", regionId);
                    continue;
                }
            }

            if (!spotPriceByRegionEntry.hasRelatedComputeTier()) {
                logger.warn("Spot price table reader: compute tier is mandatory for Spot prices: {}",
                        spotPriceByRegionEntry.toString());
                continue;
            }
            final String computeTierId = spotPriceByRegionEntry.getRelatedComputeTier().getId();
            final Long computeTierOid = cloudEntitiesMap.get(computeTierId);
            if (computeTierOid == null) {
                logger.warn("Spot price table reader: OID not found for compute tier ID {}",
                        computeTierId);
                continue;
            }

            // If guest OS type is not set it is expected to return UNKNOWN_OS
            final OSType osType = spotPriceByRegionEntry.getGuestOsType();

            logger.trace("Adding Spot price for zone/region {}, compute tier {}, OS {}",
                    () -> zoneOrRegionOid, () -> computeTierOid, () -> osType);

            final Price price = spotPriceByRegionEntry.getPrice();
            final Map<OSType, Price> spotPriceByOsType =
                    spotPriceMap.computeIfAbsent(zoneOrRegionOid, (k) -> new HashMap<>())
                            .computeIfAbsent(computeTierOid, (k) -> new EnumMap<>(OSType.class));

            final Price existingPrice = spotPriceByOsType.get(osType);
            if (spotPriceByOsType.containsKey(osType)) {
                Level logLevel = existingPrice != null && price != null && existingPrice.getPriceAmount() != null
                                && price.getPriceAmount() != null
                                && existingPrice.getPriceAmount().getAmount() != price.getPriceAmount().getAmount()
                                ? Level.DEBUG : Level.WARN;
                logger.log(logLevel, "Spot price table reader: duplicate price for zone/region {},"
                                                + " compute tier {}, OS {}: existing price - {}, new price - {}",
                                zoneOrRegionOid, computeTierOid, osType, existingPrice, price);
                continue;
            }

            spotPriceByOsType.put(osType, price);
        }
        return spotPriceMap;
    }

    /**
     * Convert Spot prices map to the map of {@code SpotInstancePriceTable}.
     *
     * @param spotPriceMap Source map with Spot prices.
     * @return Map of Zone/Region OID -> {@link SpotInstancePriceTable}.
     */
    private Map<Long, SpotInstancePriceTable> convertToSpotInstancePriceTableMap(
            @Nonnull final Map<Long, Map<Long, Map<OSType, Price>>> spotPriceMap) {
        final Map<Long, SpotInstancePriceTable> result = new HashMap<>();
        spotPriceMap.forEach((zoneOrRegionOid, priceByComputeTierOid) -> {
            final SpotInstancePriceTable.Builder spotInstancePriceTable =
                    SpotInstancePriceTable.newBuilder();
            priceByComputeTierOid.forEach((computeTierOid, priceByOsType) -> {
                final SpotPricesForTier.Builder spotPricesForTier = SpotPricesForTier.newBuilder();
                priceByOsType.forEach((osType, price) ->
                        spotPricesForTier.addPriceForGuestOsType(PriceForGuestOsType.newBuilder()
                            .setGuestOsType(osType)
                            .setPrice(price)));
                spotInstancePriceTable.putSpotPricesByTierOid(computeTierOid,
                        spotPricesForTier.build());
            });
            result.put(zoneOrRegionOid, spotInstancePriceTable.build());
        });
        return result;
    }
}
