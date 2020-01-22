package com.vmturbo.cost.calculation.topology;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceByOsEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceByOsEntry.LicensePrice;

/**
 * A class representing the Pricing Data for a particular account. An a
 */
public class AccountPricingData {

    private final DiscountApplicator discountApplicator;

    private final PriceTable priceTable;

    private final Map<OSType, List<LicensePrice>> onDemandlicensePrices;

    private final Map<OSType, List<LicensePrice>> reservedLicensePrices;

    private final Map<OSType, Map<Integer, Optional<LicensePrice>>>
            licensePriceByOsTypeByNumCores = Maps.newHashMap();

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
    public AccountPricingData(DiscountApplicator discountApplicator,
                              final PriceTable priceTable, Long accountPricingDataOid) {
        this.discountApplicator = discountApplicator;
        this.priceTable = priceTable;
        this.onDemandlicensePrices = priceTable.getOnDemandLicensePricesList().stream()
                .collect(Collectors.toMap(LicensePriceByOsEntry::getOsType,
                        LicensePriceByOsEntry::getLicensePricesList,
                        // if there are duplicate OS types then merge their price lists
                        (list1, list2) -> {
                            List<LicensePrice> list3 = Lists.newArrayList();
                            list3.addAll(list1);
                            list3.addAll(list2);
                            return list3;
                        }));

        this.reservedLicensePrices = priceTable.getReservedLicensePricesList().stream()
                .collect(Collectors.toMap(LicensePriceByOsEntry::getOsType,
                        LicensePriceByOsEntry::getLicensePricesList,
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
     * @return the matching license price
     */
    private Optional<LicensePrice> getExplicitLicensePrice(OSType os, int numCores) {
        List<LicensePrice> prices = onDemandlicensePrices.get(os);
        if (prices == null) {
            return Optional.empty();
        }
        // Lazily populate licensePriceByOsTypeByNumCores
        Map<Integer, Optional<LicensePrice>> perOsEntry =
                licensePriceByOsTypeByNumCores.computeIfAbsent(os, key -> Maps.newHashMap());
        return perOsEntry.computeIfAbsent(numCores, n -> prices.stream()
                .filter(license -> license.getNumberOfCores() >= n)
                .min(Comparator.comparing(LicensePrice::getNumberOfCores)));
    }

    private Optional<LicensePrice> getReservedLicensePrice(OSType os, int numCores) {
        List<LicensePrice> prices = reservedLicensePrices.get(os);
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
     * @return the matching license price
     */
    @Nonnull
    public LicensePriceTuple getLicensePriceForOS(OSType os,
                                                  int numOfCores,
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
        getExplicitLicensePrice(os, numOfCores)
                .ifPresent(licenseExplicitPrice -> licensePrice
                        .setExplicitOnDemandLicensePrice(licenseExplicitPrice.getPrice()
                                .getPriceAmount().getAmount()));
        getReservedLicensePrice(os, numOfCores)
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
                .filter(computeTierConfigPrice -> computeTierConfigPrice.getGuestOsType() == os)
                .findAny();
    }

    public PriceTable getPriceTable() {
        return this.priceTable;
    }

    public DiscountApplicator<TopologyEntityDTO> getDiscountApplicator() {
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
}

