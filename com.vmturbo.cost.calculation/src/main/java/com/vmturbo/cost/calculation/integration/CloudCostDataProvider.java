package com.vmturbo.cost.calculation.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.AccountPricingData.DebugInfoNeverUsedInCode;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cloud.common.topology.CloudTopology;

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
     * @param cloudTopo The cloud topology
     * @param topologyEntityInfoExtractor The topolog entity info extractor.
     *
     * @return The {@link CloudCostData}.
     * @throws CloudCostDataRetrievalException If there is an error retrieving the data.
     */
    @Nonnull
    CloudCostData getCloudCostData(@Nonnull TopologyInfo topoInfo, CloudTopology<TopologyEntityDTO> cloudTopo,
                                   @Nonnull TopologyEntityInfoExtractor topologyEntityInfoExtractor) throws CloudCostDataRetrievalException;

    /**
     * The bundle of non-topology data required to compute costs. This can include things like
     * the {@link PriceTable} from the cost probes, the discounts for various business accounts,
     * and the reserved instance coverage.
     *
     * @param <T> The class used to represent entities in the topology. For example,
     *            TopologyEntityDTO for the real time topology.
     */
    @Immutable
    class CloudCostData<T> {

        private static final Logger logger = LogManager.getLogger();

        private static final CloudCostData EMPTY = new CloudCostData<>(Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Optional.empty());

        private final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId;

        private final Map<Long, ReservedInstanceData> riBoughtDataById;

        private final Map<Long, ReservedInstanceData> buyRIBoughtDataById;

        private final Map<Long, AccountPricingData<T>> accountPricingDataByBusinessAccountOid;

        private final Map<Long, EntityReservedInstanceCoverage> filteredRiCoverageByEntityId;

        /**
         * Map to hold each entity's uptime percentage.
         */
        public final Map<Long, EntityUptimeDTO> entityUptimeByEntityId;
        private final EntityUptimeDTO defaultEntityUptime;


        public CloudCostData(@Nonnull final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId,
                             @Nonnull final Map<Long, EntityReservedInstanceCoverage> filteredRiCoverageByEntityId,
                             @Nonnull final Map<Long, ReservedInstanceBought> riBoughtById,
                             @Nonnull final Map<Long, ReservedInstanceSpec> riSpecById,
                             @Nonnull final Map<Long, ReservedInstanceBought> buyRIBoughtById,
                             @Nonnull final Map<Long, AccountPricingData<T>>
                                     accountPricingDataByBusinessAccountOid,
                             @Nonnull final Map<Long, EntityUptimeDTO> entityUptimeByEntityId,
                             @Nonnull final Optional<EntityUptimeDTO> defaultEntityUptime) {
            this.riCoverageByEntityId = Objects.requireNonNull(riCoverageByEntityId);
            this.accountPricingDataByBusinessAccountOid = Objects.requireNonNull(accountPricingDataByBusinessAccountOid);
            this.filteredRiCoverageByEntityId = Objects.requireNonNull(filteredRiCoverageByEntityId);
            // Combine RI Bought and RI Specs.
            this.riBoughtDataById = riBoughtById.values().stream()
                    .filter(riBought -> riSpecById.containsKey(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec()))
                    .map(riBought -> new ReservedInstanceData(riBought, riSpecById.get(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec())))
                    .collect(Collectors.toMap(riData -> riData.getReservedInstanceBought().getId(), Function.identity()));
            this.buyRIBoughtDataById = buyRIBoughtById.values().stream()
                    .filter(riBought -> riSpecById.containsKey(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec()))
                    .map(riBought -> new ReservedInstanceData(riBought, riSpecById.get(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec())))
                    .collect(Collectors.toMap(riData -> riData.getReservedInstanceBought().getId(), Function.identity()));
            this.entityUptimeByEntityId = entityUptimeByEntityId;
            this.defaultEntityUptime = defaultEntityUptime.orElse(
                    EntityUptimeDTO.newBuilder().setUptimePercentage(100D).build());
        }


            /**
             * Get the account pricing data corresponding to the business account oid.
             *
             * @param businessAccountOid The business account oid.
             *
             * @return The account pricing data corresponding to the business account oid.
             */
        public Optional<AccountPricingData<T>> getAccountPricingData(Long businessAccountOid) {
            return Optional.ofNullable(accountPricingDataByBusinessAccountOid.get(businessAccountOid));
        }

        /**
         * This method is used to log missing price tables referenced in the account pricing data.
         */
        public void logMissingAccountPricingData() {
            for (AccountPricingData accountPricingData: accountPricingDataByBusinessAccountOid.values()) {
                DebugInfoNeverUsedInCode debugInfoNeverUsedInCode = accountPricingData.getDebugInfoNeverUsedInCode();
                if (accountPricingData.getPriceTable() == null) {
                    logger.error("ERROR!!: Account Pricing Data {} with price table key oid {}"
                                    + " created by business account {} missing price table. This may happen if cost data has not successfully been uploaded.",
                            accountPricingData.getAccountPricingDataOid(), debugInfoNeverUsedInCode.getPriceTableKeyOid(), debugInfoNeverUsedInCode.getRepresentativeAccountOid());
                    return;
                }
                if (accountPricingData.getPriceTable().getOnDemandPriceByRegionIdMap() == null || accountPricingData.getPriceTable().getOnDemandPriceByRegionIdMap().isEmpty()) {
                    logger.error("ERROR!!: Account Pricing Data {} with price table key oid {} created by business account {} has no on demand pricing in price table.. This may happen if cost data has not successfully been uploaded.",
                            accountPricingData.getAccountPricingDataOid(), debugInfoNeverUsedInCode.getPriceTableKeyOid(), debugInfoNeverUsedInCode.getRepresentativeAccountOid());
                    return;
                }
            }
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

        @Nonnull
        public Optional<ReservedInstanceData> getBuyRIData(final long riBoughtId) {
            return Optional.ofNullable(buyRIBoughtDataById.get(riBoughtId));
        }

        @Nonnull
        public Optional<EntityReservedInstanceCoverage> getFilteredRiCoverage(final long entityId) {
            return Optional.ofNullable(filteredRiCoverageByEntityId.get(entityId));
        }

        @Nonnull
        public Map<Long, EntityReservedInstanceCoverage> getFilteredRiCoverageByEntityId() {
            return filteredRiCoverageByEntityId;
        }

        /**
         * This will return a read-only collection of {@link ReservedInstanceData} representing
         * all the existing RIs in the inventory. This will be used in real time analysis.
         *
         * @return Collection of {@link ReservedInstanceData} representing the existing RIs.
         */
        @Nonnull
        public Collection<ReservedInstanceData> getExistingRiBought() {
            return Collections.unmodifiableCollection(riBoughtDataById.values());
        }

        /**
         * This will return a read-only collection of {@link ReservedInstanceData} representing
         * all the existing RIs in the inventory and the Buy RI recommendations. This will be used
         * during Optimize Cloud Plans.
         *
         * @return Read-only Collection of {@link ReservedInstanceData} representing the existing
         * RIs bought and the Buy RI recommendations
         */
        @Nonnull
        public Collection<ReservedInstanceData> getAllRiBought() {
            List<ReservedInstanceData> allRiData = new ArrayList<>();
            allRiData.addAll(riBoughtDataById.values());
            // RIs from buy RI analysis are already coming part of riBoughtByDataId for OCP. Avoid adding the same duplicate RI from the buyRIBoughtDatabyID
            for (Entry<Long, ReservedInstanceData> entry : buyRIBoughtDataById.entrySet()) {
                final Long buyRIId = entry.getKey();
                if (!riBoughtDataById.containsKey(buyRIId)) {
                    allRiData.add(entry.getValue());
                }
            }
            return Collections.unmodifiableCollection(allRiData);
        }

        /**
         * Gets the Buy RI instances within scope of they topology
         *
         * @return A read-only collection of {@link ReservedInstanceData} representing
         * all buy RI recommendations.
         */
        @Nonnull
        public Collection<ReservedInstanceData> getAllBuyRIs() {
            return Collections.unmodifiableCollection(buyRIBoughtDataById.values());
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


        /**
         * Get the entity uptime percentage for a given entity id.
         * @param entityOid The entity OID
         * @return EntityUptime percentage
         */
        @Nonnull
        public Double getEntityUptimePercentage(final long entityOid) {
            EntityUptimeDTO entityUptimeDTO = entityUptimeByEntityId
                    .getOrDefault(entityOid, defaultEntityUptime);
            return entityUptimeDTO.getUptimePercentage();
        }

        /**
         * Get the entity uptime for a given entity id.
         * @param entityOid The entity OID
         * @return EntityUptime
         */
        @Nonnull
        public EntityUptimeDTO getEntityUptime(final long entityOid) {
            return entityUptimeByEntityId
                    .getOrDefault(entityOid, defaultEntityUptime);
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

        @Override
        public int hashCode() {
            return Objects.hash(reservedInstanceSpec.getId(), reservedInstanceBought.getId());
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof ReservedInstanceData) {
                final ReservedInstanceData otherRIData = (ReservedInstanceData)o;
                if (reservedInstanceSpec.getId() == otherRIData.reservedInstanceSpec.getId() &&
                        reservedInstanceBought.getId() == otherRIData.reservedInstanceBought.getId()) {
                    return true;
                }
            }

            return false;
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
        private double implicitOnDemandLicensePrice;

        /**
         * a catalog license price
         */
        private double explicitOnDemandLicensePrice;

        /**
         * License price portion coming from the RI coverage.
         */
        private double reservedInstanceLicensePrice;

        public LicensePriceTuple() {
            implicitOnDemandLicensePrice = NO_LICENSE_PRICE;
            explicitOnDemandLicensePrice = NO_LICENSE_PRICE;
            reservedInstanceLicensePrice = NO_LICENSE_PRICE;
        }

        /**
         * Get the implicit license price
         * @return the implicit license price of the tier
         */
        public double getImplicitOnDemandLicensePrice() {
            return implicitOnDemandLicensePrice;
        }

        /**
         * Get the explicit license price
         * @return the explicit license price of the tier
         */
        public double getExplicitOnDemandLicensePrice() {
            return explicitOnDemandLicensePrice;
        }

        /**
         * Get the reserved instance license price.
         *
         * @return A double representing the reserved instance license price.
         */
        public double getReservedInstanceLicensePrice() {
            return reservedInstanceLicensePrice;
        }

        /**
         * Set the implicit license price
         * @param implicitPrice the implicit license price (template specific)
         */
        public void setImplicitOnDemandLicensePrice(double implicitPrice) {
            implicitOnDemandLicensePrice = implicitPrice;
        }

        /**
         * Set the explicit license price.
         *
         * @param explicitPrice the explicit license price (based on number of cores)
         */
        public void setExplicitOnDemandLicensePrice(double explicitPrice) {
            explicitOnDemandLicensePrice = explicitPrice;
        }

        /**
         * Set the license price of the RI coverage associated with the VM.
         *
         * @param reservedLicensePrice The reserved instance license price.
         */
        public void setReservedInstanceLicensePrice(double reservedLicensePrice) {
            reservedInstanceLicensePrice = reservedLicensePrice;
        }
    }
}
