package com.vmturbo.cost.calculation.topology;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;

/**
 * A class representing the Pricing Data for a particular account.
 *
 * @param <T> The class used to represent entities in the topology. For example,
 *            TopologyEntityDTO for the real time topology.
 */
public class AccountPricingData<T> {

    private final DiscountApplicator<T> discountApplicator;

    private final PriceTable priceTable;

    //List of licensePrice is sorted by number of cores.
    private final Map<LicenseIdentifier, Collection<LicensePrice>> onDemandLicensePrices;

    private final Map<LicenseIdentifier, List<LicensePrice>> reservedLicensePrices;

    private final Long accountPricingDataOid;

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
            .put(OSType.LINUX_WITH_SQL_ENTERPRISE, Optional.of(OSType.LINUX))
            .put(OSType.LINUX_WITH_SQL_STANDARD, Optional.of(OSType.LINUX))
            .put(OSType.LINUX_WITH_SQL_WEB, Optional.of(OSType.LINUX))
            .put(OSType.WINDOWS_SERVER, Optional.of(OSType.WINDOWS))
            .put(OSType.WINDOWS_SERVER_BURST, Optional.of(OSType.WINDOWS))
            .put(OSType.WINDOWS_WITH_SQL_ENTERPRISE, Optional.of(OSType.WINDOWS))
            .put(OSType.WINDOWS_WITH_SQL_STANDARD, Optional.of(OSType.WINDOWS))
            .put(OSType.WINDOWS_WITH_SQL_WEB, Optional.of(OSType.WINDOWS)).build();

    /**
     * Constructor for the account pricing data.
     *
     * @param discountApplicator The discount applicator.
     * @param priceTable The price table.
     * @param accountPricingDataOid The account pricing data oid.
     */
    public AccountPricingData(DiscountApplicator<T> discountApplicator,
                              final PriceTable priceTable, Long accountPricingDataOid) {
        this.discountApplicator = discountApplicator;
        this.priceTable = priceTable;
        this.onDemandLicensePrices = priceTable.getOnDemandLicensePricesList().stream()
                .collect(Collectors.toMap(licensePriceEntry -> new LicenseIdentifier(licensePriceEntry.getOsType(),
                        licensePriceEntry.getBurstableCPU()),
                        LicensePriceEntry::getLicensePricesList,
                        // if there are duplicate OS types then merge their price lists
                        (list1, list2) -> {
                            List<LicensePrice> list3 = Lists.newLinkedList();
                            list3.addAll(list1);
                            list3.addAll(list2);
                            list3.sort(Comparator.comparingInt(LicensePrice::getNumberOfCores));
                            return list3;
                        }));

        this.reservedLicensePrices = priceTable.getReservedLicensePricesList().stream()
                .collect(Collectors.toMap(licensePriceEntry -> new LicenseIdentifier(licensePriceEntry.getOsType(),
                        licensePriceEntry.getBurstableCPU()),
                        LicensePriceEntry::getLicensePricesList,
                        (list1, list2) -> {
                            List<LicensePrice> list3 = Lists.newArrayList();
                            list3.addAll(list1);
                            list3.addAll(list2);
                            return list3;
                        }));
        this.accountPricingDataOid = accountPricingDataOid;
    }

    /**
     * Return the license price that matches the OS and has the minimal number of
     * cores that is .GE. the numCores argument. If numCores is too high then
     * return {@link Optional#empty}. This will have effect on Azure instances.
     *
     * @param os VM OS
     * @param numCores VM number of CPUs
     * @param burstableCPU if a license support burstableCPU.
     * @return the matching license price
     */
    private Optional<LicensePrice> getExplicitLicensePrice(OSType os, int numCores, boolean burstableCPU) {
        LicenseIdentifier licenseIdentifier = new LicenseIdentifier(os, burstableCPU);
        // the prices are already ASC sorted by num of cores in licensePrice.
        Collection<LicensePrice> prices = onDemandLicensePrices.get(licenseIdentifier);
        if (prices == null) {
            return Optional.empty();
        } else {
            return prices.stream().filter(price -> price.getNumberOfCores() >= numCores).findFirst();
        }
    }

    private Optional<LicensePrice> getReservedLicensePrice(OSType os, int numCores, final boolean burstableCPU) {
        List<LicensePrice> prices = reservedLicensePrices.get(new LicenseIdentifier(os, burstableCPU));
        if (prices == null) {
            return Optional.empty();
        }
        return prices.stream().filter(s -> s.getNumberOfCores() == numCores).findFirst();
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
     * @param numOfCores the number of cores of that template
     * @param computePriceList all compute prices for this specific template
     * @param burstableCPU if a license support burstableCPU.
     * @return the matching license price
     */
    @Nonnull
    public LicensePriceTuple getLicensePrice(OSType os,
                                             final int numOfCores,
                                             ComputeTierPriceList computePriceList,
                                             final boolean burstableCPU) {
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
        getExplicitLicensePrice(os, numOfCores, burstableCPU)
                .ifPresent(licenseExplicitPrice -> licensePrice
                        .setExplicitOnDemandLicensePrice(licenseExplicitPrice.getPrice()
                                .getPriceAmount().getAmount()));
        getReservedLicensePrice(os, numOfCores, burstableCPU)
                .ifPresent(reservedLicensePrice -> licensePrice
                        .setReservedInstanceLicensePrice(reservedLicensePrice.getPrice().getPriceAmount().getAmount()));

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
    private static class LicenseIdentifier {
        private final OSType osType;
        private final boolean burstableCPU;

        private LicenseIdentifier(final OSType osType, final boolean burstableCPU) {
            this.osType = osType;
            this.burstableCPU = burstableCPU;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof LicenseIdentifier)) {
                return false;
            }
            return ((LicenseIdentifier)other).getOsType().equals(this.getOsType()) &&
                    ((LicenseIdentifier)other).isBurstableCPU() == this.isBurstableCPU();
        }

        @Override
        public int hashCode() {
            return Objects.hash(osType, burstableCPU);
        }

        public OSType getOsType() {
            return osType;
        }

        public boolean isBurstableCPU() {
            return burstableCPU;
        }
    }

}

