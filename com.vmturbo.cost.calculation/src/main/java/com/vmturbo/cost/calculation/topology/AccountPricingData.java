package com.vmturbo.cost.calculation.topology;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.components.common.utils.OptionalUtils;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeTierConfig;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverride;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverrides;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;

/**
 * A class representing the Pricing Data for a particular account.
 *
 * @param <T> The class used to represent entities in the topology. For example,
 *            TopologyEntityDTO for the real time topology.
 */
public class AccountPricingData<T> {

    private static final Comparator<LicensePrice> LICENSE_PRICE_COMPARATOR =
            Comparator.comparing(LicensePrice::getNumberOfCores)
                    .thenComparing(LicensePrice::hashCode);

    private static final Logger logger = LogManager.getLogger();

    private final DiscountApplicator<T> discountApplicator;

    private final PriceTable priceTable;

    //List of licensePrice is sorted by number of cores.
    private final Map<LicenseIdentifier, SortedLicensePriceEntry> onDemandLicensePrices;

    //List of licensePrice is sorted by number of cores.
    private final Map<LicenseIdentifier, SortedLicensePriceEntry> reservedLicensePrices;

    // List of on-demand license override values by the (compute tier OID, OSType) tuple
    private final Map<ComputeTierOSType, LicenseOverride> onDemandLicenseOverrideMap;

    // List of reserved license override values by the (compute tier OID, OSType) tuple
    private final Map<ComputeTierOSType, LicenseOverride> reservedLicenseOverrideMap;

    private final Long accountPricingDataOid;

    private final DebugInfoNeverUsedInCode debugInfoNeverUsedInCode;

    /**
     * This map specifies which is the base OS for each OS type.
     * It is used for license price calculation in case that the OS doesn't have an entry in the
     * price adjustment list (in which case we will search for the base OSs price adjustment.
     */
    public static final Map<OSType, Optional<OSType>> OS_TO_BASE_OS = ImmutableMap.<OSType, Optional<OSType>>builder()
            .put(OSType.UNKNOWN_OS, Optional.empty())
            .put(OSType.LINUX, Optional.empty())
            .put(OSType.WINDOWS, Optional.empty())
            .put(OSType.WINDOWS_BYOL, Optional.empty())
            .put(OSType.SUSE, Optional.of(OSType.LINUX))
            .put(OSType.RHEL, Optional.of(OSType.LINUX))
            .put(OSType.UBUNTU_PRO, Optional.of(OSType.LINUX))
            .put(OSType.LINUX_WITH_SQL_ENTERPRISE, Optional.of(OSType.LINUX))
            .put(OSType.LINUX_WITH_SQL_STANDARD, Optional.of(OSType.LINUX))
            .put(OSType.LINUX_WITH_SQL_WEB, Optional.of(OSType.LINUX))
            .put(OSType.WINDOWS_WITH_SQL_ENTERPRISE, Optional.of(OSType.WINDOWS))
            .put(OSType.WINDOWS_WITH_SQL_STANDARD, Optional.of(OSType.WINDOWS))
            .put(OSType.WINDOWS_WITH_SQL_WEB, Optional.of(OSType.WINDOWS)).build();

    /**
     * Collector to group prices by {@link LicenseIdentifier} and sort prices by number of cores.
     */
    private static final Collector<LicensePriceEntry, ?, Map<LicenseIdentifier, SortedLicensePriceEntry>>
            PRICE_COLLECTOR = Collectors.toMap(
            licensePriceEntry -> LicenseIdentifier.of(licensePriceEntry.getOsType(), licensePriceEntry.getBurstableCPU()),
            entry -> SortedLicensePriceEntry.builder()
                    // entry.getBaseOsType() will return the default enum of UNKNOWN_OS. In order to avoid
                    // spurious lookups of base rates, first check whether base OS type is set.
                    .baseOSType(entry.hasBaseOsType()
                            ? Optional.of(entry.getBaseOsType())
                            : Optional.empty())
                    .licensePrices(ImmutableSortedSet.orderedBy(LICENSE_PRICE_COMPARATOR)
                            .addAll(entry.getLicensePricesList())
                            .build())
                    .build(),
            // if there are duplicate OS types then merge their price lists
            (sortedEntryA, sortedEntryB) -> {
                if (sortedEntryA.baseOSType().equals(sortedEntryB.baseOSType())) {
                    return SortedLicensePriceEntry.builder()
                            .baseOSType(sortedEntryA.baseOSType())
                            .licensePrices(ImmutableSortedSet.orderedBy(LICENSE_PRICE_COMPARATOR)
                                    .addAll(sortedEntryA.licensePrices())
                                    .addAll(sortedEntryB.licensePrices())
                                    .build())
                            .build();

                } else {
                    // Log a warning - there is no contextual data available, in order to indicate
                    // the conflicting price, given this is only available in the LicensePriceEntry.
                    // However, this is preferable to not addressing potential conflicts, which would
                    // invalidate all license pricing data.
                    logger.warn("Duplicate conflicting license price entries found");
                    return SortedLicensePriceEntry.EMPTY_LICENSE_PRICE_ENTRY;
                }
            });

    /**
     * Constructor for the account pricing data.
     *
     * @param discountApplicator The discount applicator.
     * @param priceTable The price table.
     * @param accountPricingDataOid The account pricing data oid.
     * @param priceTableOid The price table key oid.
     * @param businessAccountOid The original business account oid which created this account pricing data.
     */
    public AccountPricingData(DiscountApplicator<T> discountApplicator, final PriceTable priceTable,
            Long accountPricingDataOid, long priceTableOid, long businessAccountOid) {
        this.discountApplicator = discountApplicator;
        this.priceTable = priceTable;
        this.onDemandLicensePrices = priceTable.getOnDemandLicensePricesList()
                .stream()
                .collect(Collectors.collectingAndThen(PRICE_COLLECTOR, ImmutableMap::copyOf));
        this.reservedLicensePrices = priceTable.getReservedLicensePricesList()
                .stream()
                .collect(Collectors.collectingAndThen(PRICE_COLLECTOR, ImmutableMap::copyOf));
        this.accountPricingDataOid = accountPricingDataOid;
        this.onDemandLicenseOverrideMap = AccountPricingData.convertLicenseOverrides(
                priceTable.getOnDemandLicenseOverridesMap(),
                "On-demand License Overrides");
        this.reservedLicenseOverrideMap = AccountPricingData.convertLicenseOverrides(
                priceTable.getReservedLicenseOverridesMap(),
                "Reserved License Overrides");
        this.debugInfoNeverUsedInCode = new DebugInfoNeverUsedInCode(priceTableOid, businessAccountOid);
    }

    private static Map<ComputeTierOSType, LicenseOverride> convertLicenseOverrides(
            @Nonnull Map<Long, LicenseOverrides> licenseOverridesMap,
            @Nonnull String licenseOverrideTag) {

        return licenseOverridesMap.entrySet()
                .stream()
                .flatMap(overrideEntries -> overrideEntries.getValue().getLicenseOverrideList()
                        .stream()
                        .map(overrideEntry -> Pair.of(overrideEntries.getKey(), overrideEntry)))
                .collect(ImmutableMap.toImmutableMap(
                        overridePair -> ComputeTierOSType.of(
                                overridePair.getKey(), overridePair.getValue().getOsType()),
                        Pair::getValue,
                        (overrideA, overrideB) -> {
                            logger.warn("Duplicate license overrides for in parsing {}:\n{}\n{}",
                                    licenseOverrideTag, overrideA, overrideB);
                            return overrideA;
                        }));
    }

    /**
     * Return the explicit license price that matches the OS and has the minimal number of
     * cores that is .GE. the numCores argument. If numCores is too high then
     * return {@link Optional#empty}. This will have effect on Azure instances.
     *
     * @param tierConfig The compute tier config.
     * @param os The os.
     * @return the matching license price
     */
    private Optional<CurrencyAmount> getExplicitLicensePrice(@Nonnull ComputeTierConfig tierConfig,
                                                             @Nonnull OSType os) {
        return getLicensePrice(tierConfig, os, onDemandLicensePrices, onDemandLicenseOverrideMap);
    }

    /**
     * Return the license price of the RI coverage that matches the OS and has the minimal number of
     * cores that is .GE. the numCores argument. If numCores is too high then
     * return {@link Optional#empty}. This will have effect on Azure instances.
     *
     * @param tierConfig The compute tier config.
     * @param os The os.
     * @return the matching license price
     */
    public Optional<CurrencyAmount> getReservedLicensePrice(@Nonnull ComputeTierConfig tierConfig,
                                                            @Nonnull OSType os) {
        return getLicensePrice(tierConfig, os, reservedLicensePrices, reservedLicenseOverrideMap);
    }

    /**
     * Return the  license price that matches the OS and has the minimal number of
     * cores that is .GE. the numCores argument. If numCores is too high then
     * return {@link Optional#empty}.
     *
     * @param os VM OS
     * @return the matching license price
     */
    private Optional<CurrencyAmount> getLicensePrice(@Nonnull ComputeTierConfig tierConfig,
                                                     @Nonnull OSType os,
                                                     Map<LicenseIdentifier, SortedLicensePriceEntry> priceMapping,
                                                     Map<ComputeTierOSType, LicenseOverride> licenseOverrideMap) {

        final ComputeTierOSType computeTierOSType = ComputeTierOSType.of(tierConfig.computeTierOid(), os);
        final int numCores = licenseOverrideMap.containsKey(computeTierOSType)
                // Right now, we assume if an override is provided, it will override the number
                // of cores.
                ? licenseOverrideMap.get(computeTierOSType).getOverrideValue().getNumCores()
                : tierConfig.numCores();
        final boolean isBurstableCPU = tierConfig.isBurstableCPU();

        SortedLicensePriceEntry licensePriceEntry = priceMapping.get(LicenseIdentifier.of(os, isBurstableCPU));
        if (licensePriceEntry == null) {
            return Optional.empty();
        }

        final Optional<CurrencyAmount> price = licensePriceEntry.licensePrices()
                .stream()
                .filter(s -> s.getNumberOfCores() >= numCores)
                .map(LicensePrice::getPrice)
                .map(Price::getPriceAmount)
                .findFirst();

        final Optional<CurrencyAmount> basePrice = licensePriceEntry.baseOSType()
                .flatMap(baseOsType -> getLicensePrice(tierConfig, baseOsType, priceMapping, licenseOverrideMap));

        return OptionalUtils.reduce(AccountPricingData::mergeCurrencyAmounts, price, basePrice);
    }

    private static CurrencyAmount mergeCurrencyAmounts(@Nonnull CurrencyAmount ammountA,
                                                @Nonnull CurrencyAmount ammountB) {

        Preconditions.checkArgument(ammountA.getCurrency() == ammountB.getCurrency());

        return CurrencyAmount.newBuilder()
                .setCurrency(ammountA.getCurrency())
                .setAmount(ammountA.getAmount() + ammountB.getAmount())
                .build();
    }

    /**
     * Return the license price that matches the OS for a specific template.
     *
     * <p>The license price can be constructed in 3 different ways:
     *
     * <p>1) Price adjustment only (explicit price).
     * <ul>
     *  <li>e.g: In Azure, when the OS is Windows.
     *  <li>In AWS, all OSs besides the base OS.
     * </ul>
     *
     * <p>2) License price only (implicit price)
     * <ul>
     * <li>e.g: The OS is based on Linux (such as RHEL).
     * <li>RHEL doesn't have an entry in the price adjustment lists in Azure templates.
     * <li>Since RHEL is based on Linux (which is the base OS) we will not find a
     * <li>price adjustments for Linux, so we'll consider only the price from
     * <li>Price Table's LicensePrices list.
     * </ul>
     *
     * <p>3) Price adjustment + license price (from the Price Table's LicensePrices list)
     * <ul>
     *  <li>e.g: The OS is based on Windows (such as Windows SQL Enterprise).
     *  <li>Windows SQL Enterprise doesn't have an entry in the price adjustment lists in
     *  <li>Azure templates.
     *  <li>The price is the sum of:
     *  <ul>
     *      <li>- Windows price taken from the price adjustment.
     *      <li>- Windows SQL Enterprise price taken from the Price Table's LicensePrices list.
     *  </ul>
     * </ul>
     *
     * @param os the OS for which we want to get the price of for the template
     * @param computePriceList all compute prices for this specific template
     * @return the matching license price
     */
    @Nonnull
    public LicensePriceTuple getLicensePrice(ComputeTierConfig tierConfig,
                                             OSType os,
                                             ComputeTierPriceList computePriceList) {
        LicensePriceTuple licensePrice = new LicensePriceTuple();

        // calculate the implicit price by getting the price adjustment of the current OS.
        // if not present, get the price adjustment for the base OS.
        // the current OS is the same as the base OS, no need to add explicit price
        if (computePriceList.getBasePrice() != null && os != computePriceList.getBasePrice().getGuestOsType()) {
            licensePrice.setImplicitOnDemandLicensePrice(
                    getOsPriceAdjustment(os, computePriceList)
                            .map(computeTierConfigPrice -> computeTierConfigPrice.getPricesList()
                                    .get(0).getPriceAmount().getAmount())
                            .orElseGet(() ->
                                    OS_TO_BASE_OS.get(os)
                                            .map(baseOS -> getOsPriceAdjustment(baseOS, computePriceList)
                                                    .map(baseComputeTierConfigPrice ->
                                                            baseComputeTierConfigPrice.getPricesList().get(0)
                                                                    .getPriceAmount().getAmount())
                                                    .orElse(0.0))
                                            .orElse(0.0)));
        }

        // add the price of the license itself as the explicit price
        getExplicitLicensePrice(tierConfig, os)
                .ifPresent(licenseExplicitPrice -> licensePrice
                        .setExplicitOnDemandLicensePrice(licenseExplicitPrice.getAmount()));
        getReservedLicensePrice(tierConfig, os)
                .ifPresent(reservedLicensePrice -> licensePrice
                        .setReservedInstanceLicensePrice(reservedLicensePrice.getAmount()));

        return licensePrice;
    }

    /**
     * This method gets the price adjustment of the given OS from ComputeTierPriceList,
     * and if exists, sets it as implicit LicensePrice.
     * @param os The OS for which we want to get the price
     * @param computePriceList all compute prices for this specific template
     * @return the relevant price adjustment
     */
    private Optional<ComputeTierPriceList.ComputeTierConfigPrice> getOsPriceAdjustment(OSType os,
                                                                                       ComputeTierPriceList computePriceList) {
        return computePriceList.getPerConfigurationPriceAdjustmentsList()
                .stream()
                .filter(computeTierConfigPrice -> computeTierConfigPrice.getGuestOsType() == os )
                .findAny();
    }

    public PriceTable getPriceTable() {
        return this.priceTable;
    }

    public DiscountApplicator<T> getDiscountApplicator() {
        return this.discountApplicator;
    }

    public DebugInfoNeverUsedInCode getDebugInfoNeverUsedInCode() {
        return this.debugInfoNeverUsedInCode;
    }

    public Long getAccountPricingDataOid() {
        return this.accountPricingDataOid;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof AccountPricingData)) {
            return false;
        }

        AccountPricingData otherAccountPricingData = (AccountPricingData)other;
        return this.getDiscountApplicator() == otherAccountPricingData.getDiscountApplicator()
                && this.getPriceTable() == otherAccountPricingData.getPriceTable();
    }

    @Override
    public int hashCode() {
        return Objects.hash(discountApplicator, priceTable);
    }

    /**
     * Used to identify {@link com.vmturbo.platform.sdk.common.PricingDTOREST.LicensePriceEntry.LicensePrice}.
     */
    @Style(visibility = ImplementationVisibility.PACKAGE,
            allParameters = true,
            typeImmutable = "*Tuple")
    @Immutable(lazyhash = true, builder = false)
    interface LicenseIdentifier {

        OSType getOsType();

        boolean isBurstableCPU();

        @Nonnull
        static LicenseIdentifier of(@Nonnull OSType osType,
                                    boolean isBurstableCPU) {
            return LicenseIdentifierTuple.of(osType, isBurstableCPU);
        }

    }

    @Style(visibility = ImplementationVisibility.PACKAGE,
            overshadowImplementation = true)
    @Immutable
    interface SortedLicensePriceEntry {

        SortedLicensePriceEntry EMPTY_LICENSE_PRICE_ENTRY = SortedLicensePriceEntry.builder().build();

        Optional<OSType> baseOSType();

        @Default
        default SortedSet<LicensePrice> licensePrices() {
            return Collections.emptySortedSet();
        }

        static Builder builder() {
            return new Builder();
        }

        class Builder extends ImmutableSortedLicensePriceEntry.Builder {}
    }

    @Style(visibility = ImplementationVisibility.PACKAGE,
            allParameters = true,
            typeImmutable = "*Tuple")
    @Immutable(lazyhash = true, builder = false)
    interface ComputeTierOSType {

        long computeTierOid();

        @Nonnull
        OSType osType();

        static ComputeTierOSType of(long computeTierOid,
                                    @Nonnull OSType osType) {
            return ComputeTierOSTypeTuple.of(computeTierOid, osType);
        }
    }

    /**
     * A class holding debug related info for debugging account pricing data.
     */
    public static class DebugInfoNeverUsedInCode {
        private Long priceTableKeyOid;
        private Long representativeAccountOid;

        private DebugInfoNeverUsedInCode(@Nonnull Long priceTableKeyOid, @Nonnull Long representativeAccountOid) {
            this.representativeAccountOid = representativeAccountOid;
            this.priceTableKeyOid = priceTableKeyOid;
        }

        /**
         * Getter for the business account oid which created this account pricing data.
         *
         * @return The business account oid.
         */
        public Long getRepresentativeAccountOid() {
            return representativeAccountOid;
        }

        /**
         * Getter for the price table key oid this account pricing data object reoresents.
         *
         * @return The price table key oid.
         */
        public Long getPriceTableKeyOid() {
            return priceTableKeyOid;
        }
    }

}

