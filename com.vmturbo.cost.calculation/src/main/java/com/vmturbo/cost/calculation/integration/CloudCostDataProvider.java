package com.vmturbo.cost.calculation.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceByOsEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceByOsEntry.LicensePrice;

/**
 * An interface provided by the users of the cost calculation library to get the
 * {@link CloudCostData} required for cost calculation from wherever the cost data is stored.
 *
 * For example, at the time of this writing the cost component will need to get the cost data
 * from an internal store, whereas the market component will need to get this data from the
 * cost component.
 */
public interface CloudCostDataProvider {

    /**
     * Get the cloud cost data from this particular provider. The cloud cost data is retrieved
     * in bulk via a single call.
     *
     * @param topoInfo contains information about the topology
     * @return The {@link CloudCostData}.
     * @throws CloudCostDataRetrievalException If there is an error retrieving the data.
     */
    @Nonnull
    CloudCostData getCloudCostData(@Nonnull TopologyInfo topoInfo) throws CloudCostDataRetrievalException;

    /**
     * The bundle of non-topology data required to compute costs. This can include things like
     * the {@link PriceTable} from the cost probes, the discounts for various business accounts,
     * and the reserved instance coverage.
     */
    @Immutable
    class CloudCostData {

        private static final CloudCostData EMPTY = new CloudCostData(PriceTable.getDefaultInstance(),
                Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

        private final PriceTable combinedPriceTable;

        private final Map<Long, Discount> discountsByAccount;

        private final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId;

        private final Map<Long, ReservedInstanceData> riBoughtDataById;

        private final Map<Long, ReservedInstanceData> buyRIBoughtDataById;

        private final Map<OSType, List<LicensePrice>> onDemandlicensePrices;

        private final Map<OSType, List<LicensePrice>> reservedLicensePrices;

        private final Map<OSType, Map<Integer, Optional<LicensePrice>>>
                    licensePriceByOsTypeByNumCores = Maps.newHashMap();

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

        public CloudCostData(@Nonnull final PriceTable priceTable,
                             @Nonnull final Map<Long, Discount> discountsByAccount,
                             @Nonnull final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId,
                             @Nonnull final Map<Long, ReservedInstanceBought> riBoughtById,
                             @Nonnull final Map<Long, ReservedInstanceSpec> riSpecById,
                             @Nonnull final Map<Long, ReservedInstanceBought> buyRIBoughtById) {
            this.combinedPriceTable = Objects.requireNonNull(priceTable);
            this.discountsByAccount = Objects.requireNonNull(discountsByAccount);
            this.riCoverageByEntityId = Objects.requireNonNull(riCoverageByEntityId);

            // Combine RI Bought and RI Specs.
            this.riBoughtDataById = riBoughtById.values().stream()
                .filter(riBought -> riSpecById.containsKey(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec()))
                .map(riBought -> new ReservedInstanceData(riBought, riSpecById.get(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec())))
                .collect(Collectors.toMap(riData -> riData.getReservedInstanceBought().getId(), Function.identity()));
            this.buyRIBoughtDataById = buyRIBoughtById.values().stream()
                            .filter(riBought -> riSpecById.containsKey(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec()))
                            .map(riBought -> new ReservedInstanceData(riBought, riSpecById.get(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec())))
                            .collect(Collectors.toMap(riData -> riData.getReservedInstanceBought().getId(), Function.identity()));
            onDemandlicensePrices = priceTable.getOnDemandLicensePricesList().stream()
                            .collect(Collectors.toMap(LicensePriceByOsEntry::getOsType,
                                LicensePriceByOsEntry::getLicensePricesList,
                                    // if there are duplicate OS types then merge their price lists
                                    (list1, list2) -> {
                                        List<LicensePrice> list3 = Lists.newArrayList();
                                        list3.addAll(list1);
                                        list3.addAll(list2);
                                        return list3;
                                    }));

            reservedLicensePrices = priceTable.getReservedLicensePricesList().stream()
                            .collect(Collectors.toMap(LicensePriceByOsEntry::getOsType,
                                            LicensePriceByOsEntry::getLicensePricesList,
                                            (list1, list2) -> {
                                                List<LicensePrice> list3 = Lists.newArrayList();
                                                list3.addAll(list1);
                                                list3.addAll(list2);
                                                return list3;
                                            }));
        }

        @Nonnull
        public PriceTable getPriceTable() {
            return combinedPriceTable;
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

        /**
         * <p>Return the license price that matches the OS for a specific template.
         *
         * <p>The license price can be constructed in 3 different ways:
         * <p>1) Price adjustment only (explicit price).
         * <ul>
         *  <li>e.g: In Azure, when the OS is Windows.
         *  <li>In AWS, all OSs besides the base OS.
         * </ul>
         * <p>2) License price only (implicit price)
         * <ul>
         * <li>e.g: The OS is based on Linux (such as RHEL).
         * <li>RHEL doesn't have an entry in the price adjustment lists in Azure templates.
         * <li>Since RHEL is based on Linux (which is the base OS) we will not find a
         * <li>price adjustments for Linux, so we'll consider only the price from
         * <li>Price Table's LicensePrices list.
         * </ul>
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
            if (os != computePriceList.getBasePrice().getGuestOsType()) {
                licensePrice.setImplicitLicensePrice(
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
                    .setExplicitLicensePrice(licenseExplicitPrice.getPrice()
                        .getPriceAmount().getAmount()));

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

        @Nonnull
        public Optional<Discount> getDiscountForAccount(final long accountId) {
            return Optional.ofNullable(discountsByAccount.get(accountId));
        }

        @Nonnull
        public Optional<EntityReservedInstanceCoverage> getRiCoverageForEntity(final long entityId) {
            return Optional.ofNullable(riCoverageByEntityId.get(entityId));
        }

        @Nonnull
        public Map<Long, EntityReservedInstanceCoverage> getCurrentRiCoverage() {
            return riCoverageByEntityId;
        }

        @Nonnull
        public Optional<ReservedInstanceData> getExistingRiBoughtData(final long riBoughtId) {
            return Optional.ofNullable(riBoughtDataById.get(riBoughtId));
        }

        /**
         * This will return a read only collection of {@link ReservedInstanceData} representing
         * all the existing RIs in the inventory. This will be used in real time analysis.
         *
         * @return Collection of {@link ReservedInstanceData} representing the existing RIs.
         */
        @Nonnull
        public Collection<ReservedInstanceData> getExistingRiBought() {
            return Collections.unmodifiableCollection(riBoughtDataById.values());
        }

        /**
         * This will return a read only collection of {@link ReservedInstanceData} representing
         * all the existing RIs in the inventory and the Buy RI recommendations. This will be used
         * during Optimize Cloud Plans.
         *
         * @return Read only Collection of {@link ReservedInstanceData} representing the existing
         * RIs bought and the Buy RI recommendations
         */
        @Nonnull
        public Collection<ReservedInstanceData> getAllRiBought() {
            List<ReservedInstanceData> allRiData = new ArrayList<>();
            allRiData.addAll(riBoughtDataById.values());
            allRiData.addAll(buyRIBoughtDataById.values());
            return Collections.unmodifiableCollection(allRiData);
        }

        /**
         * Utility method to create an empty {@link CloudCostData}. Useful in testing, mocking,
         * or to continue operations after {@link CloudCostDataProvider#getCloudCostData()} throws
         * an exception.
         *
         * @return An empty {@link CloudCostData}.
         */
        @Nonnull
        public static CloudCostData empty() {
            return EMPTY;
        }

    }

    /**
     * A semantically-meaningful tuple of information about an RI purchase.
     */
    @Immutable
    class ReservedInstanceData {
        /**
         * The {@link ReservedInstanceBought} object describing the RI purchase.
         */
        private final ReservedInstanceBought reservedInstanceBought;

        /**
         * The {@link ReservedInstanceSpec} object describing non-purchase-specific details about
         * the RI.
         */
        private final ReservedInstanceSpec reservedInstanceSpec;

        public ReservedInstanceData(@Nonnull final ReservedInstanceBought reservedInstanceBought,
                                    @Nonnull final ReservedInstanceSpec reservedInstanceSpec) {
            this.reservedInstanceBought = reservedInstanceBought;
            this.reservedInstanceSpec = reservedInstanceSpec;
        }

        @Nonnull
        public ReservedInstanceBought getReservedInstanceBought() {
            return reservedInstanceBought;
        }

        @Nonnull
        public ReservedInstanceSpec getReservedInstanceSpec() {
            return reservedInstanceSpec;
        }

        /**
         * Check if riData is valid.
         * There are 2 situations when riData might be invalid:
         * 1. when cost probe is sending the data even though there is no cloud target
         * 2. when you have cloud target but in the initial rounds the reserved instances
         *    are discovered before the topology entities
         * 3. when in a cloud plan, the topology is scoped and not all reserved instances
         * are in that scope.
         * @param topology the topology
         * @return true, if tierId_ of riSpec of riData is in the topology. false, if not.
         */
        public boolean isValid(@Nonnull Map<Long, TopologyEntityDTO> topology) {
            return topology.get(reservedInstanceSpec.getReservedInstanceSpecInfo().getTierId())
                    // checking region id to exclude Ri that is not within the scoped region
                    != null && topology.get(reservedInstanceSpec
                    .getReservedInstanceSpecInfo().getRegionId()) != null;
        }
    }

    /**
     * Wrapper class for exceptions encountered when retrieving {@link CloudCostData}.
     */
    class CloudCostDataRetrievalException extends Exception {

        public CloudCostDataRetrievalException(@Nonnull final Throwable cause) {
            super(cause);
        }
    }

    /**
     * This class represents a tuple of 2 types of license prices:
     * Implicit price: a calculated license price
     * Explicit price: a catalog license price
     * A license price can be constructed of either just one of the above, or both.
     */
    class LicensePriceTuple {

        public static final double NO_LICENSE_PRICE = 0.0d;

        /**
         * a calculated license price
         */
        private double implicitLicensePrice;

        /**
         * a catalog license price
         */
        private double explicitLicensePrice;

        public LicensePriceTuple() {
            implicitLicensePrice = NO_LICENSE_PRICE;
            explicitLicensePrice = NO_LICENSE_PRICE;
        }

        /**
         * Get the implicit license price
         * @return the implicit license price of the tier
         */
        public double getImplicitLicensePrice() {
            return implicitLicensePrice;
        }

        /**
         * Get the explicit license price
         * @return the explicit license price of the tier
         */
        public double getExplicitLicensePrice() {
            return explicitLicensePrice;
        }

        /**
         * Set the implicit license price
         * @param implicitPrice the implicit license price (template specific)
         */
        public void setImplicitLicensePrice(double implicitPrice) {
            implicitLicensePrice = implicitPrice;
        }

        /**
         * Set the explicit license price
         * @param explicitPrice the explicit license price (based on number of cores)
         */
        public void setExplicitLicensePrice(double explicitPrice) {
            explicitLicensePrice = explicitPrice;
        }
    }
}
