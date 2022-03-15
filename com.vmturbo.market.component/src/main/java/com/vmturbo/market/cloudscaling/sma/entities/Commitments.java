package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Static utility methods for commitments.
 */
public class Commitments {

    private static final Logger logger = LogManager.getLogger();

    private Commitments() {}

    /**
     * Gets commodities commitment amount by tier.
     *
     * @param computeTierPricingCommoditiesGetter the compute tier pricing commodities
     *         getter.
     * @param tier the tier.
     * @param <T> the type of tier entity.
     * @return the commitment amount.
     */
    @Nonnull
    public static <T> Optional<CloudCommitmentAmount> getCommoditiesCommitmentAmountByTier(
            @Nonnull final Function<T, Optional<Map<CommodityType, Double>>> computeTierPricingCommoditiesGetter,
            @Nonnull final T tier) {
        return computeTierPricingCommoditiesGetter.apply(tier).map(
                Commitments::convertComputeTierPricingCommoditiesToCommitmentAmount);
    }

    /**
     * Gets commodities commitment amount using compute tier pricing commodities mapping.
     *
     * @param computeTierPricingCommodities the compute tier pricing commodities mapping.
     * @return the commitment amount.
     */
    @Nonnull
    private static CloudCommitmentAmount convertComputeTierPricingCommoditiesToCommitmentAmount(
            @Nonnull final Map<CommodityType, Double> computeTierPricingCommodities) {
        final CloudCommitmentAmount.Builder cloudCommitmentAmount =
                CloudCommitmentAmount.newBuilder();
        computeTierPricingCommodities.entrySet().stream().map(
                e -> createCommittedCommodityBought(e.getKey(), e.getValue())).forEach(
                c -> cloudCommitmentAmount.getCommoditiesBoughtBuilder().addCommodity(c));
        return cloudCommitmentAmount.build();
    }

    @Nonnull
    private static CommittedCommodityBought.Builder createCommittedCommodityBought(
            @Nonnull final CommodityType commodityType, final double capacity) {
        return CommittedCommodityBought.newBuilder().setCommodityType(commodityType).setCapacity(
                capacity);
    }

    /**
     * Gets coupons commitment amount by tier.
     *
     * @param tier the tier.
     * @return the commitment amount.
     */
    @Nonnull
    public static CloudCommitmentAmount getCouponsCommitmentAmountByTier(
            @Nonnull final TopologyEntityDTO tier) {
        return CloudCommitmentAmount.newBuilder().setCoupons(
                tier.getTypeSpecificInfo().getComputeTier().getNumCoupons()).build();
    }

    /**
     * Resolve covered accounts by commitment.
     *
     * @param commitment the commitment.
     * @return the set of covered accounts.
     */
    @Nonnull
    public static Set<Long> resolveCoveredAccountsByCommitment(
            @Nonnull final TopologyEntityDTO commitment) {
        return commitment.getConnectedEntityListList().stream().filter(connectedEntity ->
                connectedEntity.getConnectionType() == ConnectionType.NORMAL_CONNECTION
                        && connectedEntity.getConnectedEntityType()
                        == EntityType.BUSINESS_ACCOUNT_VALUE).map(
                ConnectedEntity::getConnectedEntityId).collect(Collectors.toSet());
    }

    /**
     * Compute the current RI Coverage and the SMA specific RI Key ID.
     *
     * @param coverageAdder the coverage adder.
     * @param riOidToCoverage RI coverage information: RI ID -> # commitment amount covered
     * @param riBoughtOidToRI map from RI bought OID to SMA RI
     * @return optional of reserved instance and covered commitment amount
     */
    @Nonnull
    public static Optional<Pair<SMAReservedInstance, CloudCommitmentAmount>> computeVmCoverage(
            @Nonnull final BinaryOperator<CloudCommitmentAmount> coverageAdder,
            @Nonnull final Map<Long, CloudCommitmentAmount> riOidToCoverage,
            @Nonnull final Map<Long, SMAReservedInstance> riBoughtOidToRI) {
        CloudCommitmentAmount coverage = CommitmentAmountCalculator.ZERO_COVERAGE;
        SMAReservedInstance ri = null;
        for (final Entry<Long, CloudCommitmentAmount> coupons : riOidToCoverage.entrySet()) {
            final long riOID = coupons.getKey();
            ri = riBoughtOidToRI.get(riOID);
            if (ri == null) {
                logger.error("computeVmCoverage RI bought OID={} not found in riBoughtOidToRI",
                        riOID);
                return Optional.empty();
            }
            coverage = coverageAdder.apply(coverage, coupons.getValue());
        }
        final CloudCommitmentAmount commitmentAmount =
                ri == null ? CommitmentAmountCalculator.ZERO_COVERAGE : coverage;
        return Optional.of(new Pair<>(ri, commitmentAmount));
    }

    /**
     * Gets RI OID to the entity coverage mapping.
     *
     * @param duplicateResolver the duplicate resolver.
     * @param couponsCovered coupons covered by reserved instances.
     * @param mappings the commitment mappings.
     * @return the  coverage mapping.
     */
    @Nonnull
    public static Map<Long, CloudCommitmentAmount> getRIOidToEntityCoverage(
            @Nonnull final BinaryOperator<CloudCommitmentAmount> duplicateResolver,
            @Nonnull final Map<Long, Double> couponsCovered,
            @Nonnull final Collection<CloudCommitmentMapping> mappings) {
        return Stream.concat(couponsCovered.entrySet()
                        .stream()
                        .map(r -> Maps.immutableEntry(r.getKey(),
                                CloudCommitmentAmount.newBuilder().setCoupons(r.getValue()).build())),
                mappings.stream()
                        .map(m -> Maps.immutableEntry(m.getCloudCommitmentOid(),
                                m.getCommitmentAmount()))).collect(
                Collectors.toMap(Entry::getKey, Entry::getValue, duplicateResolver));
    }
}
