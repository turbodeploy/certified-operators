package com.vmturbo.cloud.common.topology;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility class for determining the family relationship between compute tiers.
 */
public class ComputeTierFamilyResolver {

    private static final Comparator<ComputeTierData> COMPUTE_TIER_COMPARATOR =
            Comparator.comparing(ComputeTierData::numCoupons)
                    .thenComparing(ComputeTierData::computeTierOid);

    private final Map<String, SortedSet<ComputeTierData>> computeTiersByFamily;

    private final Map<Long, ComputeTierData> computeTierDataByOid;


    private ComputeTierFamilyResolver(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

        this.computeTierDataByOid = Objects.requireNonNull(cloudTopology)
                .getAllEntitiesOfType(EntityType.COMPUTE_TIER_VALUE)
                .stream()
                .filter((computeTier) -> computeTier.getTypeSpecificInfo().getComputeTier().hasFamily())
                .collect(ImmutableMap.toImmutableMap(
                        TopologyEntityDTO::getOid,
                        (computeTier) -> {
                            final ComputeTierInfo tierInfo = computeTier.getTypeSpecificInfo().getComputeTier();
                            final String family = tierInfo.getFamily();
                            final int numCoupons = tierInfo.getNumCoupons();

                            return ImmutableComputeTierData.of(computeTier.getOid(), family, numCoupons);
                        }));

        this.computeTiersByFamily = computeTierDataByOid.values()
                .stream()
                .filter(ComputeTierData::hasCoverageFamily)
                .collect(Collectors.groupingBy(
                        ComputeTierData::coverageFamily,
                        Collectors.toCollection(
                                () -> new TreeSet<>(COMPUTE_TIER_COMPARATOR))));
    }

    /**
     * Returns all siblings within the same compute tier, including the target {@code tierOid}.
     * @param tierOid The target tier OID.
     * @return The OIDs of all siblings within the same compute tier as the {@code tierOid}. The set
     * will contain only the {@code tierOid}, if the tier is not associated with a family. If the
     * {@code tierOid} cannot be found in the underlying topology, an empty set will be returned.
     */
    @Nonnull
    public Set<Long> getSiblingsInFamily(long tierOid) {
        if (computeTierDataByOid.containsKey(tierOid)) {
            final ComputeTierData computeTierData = computeTierDataByOid.get(tierOid);

            if (computeTiersByFamily.containsKey(computeTierData.coverageFamily())) {
                return computeTiersByFamily.get(computeTierData.coverageFamily())
                        .stream()
                        .map(ComputeTierData::computeTierOid)
                        .collect(Collectors.<Long, Set<Long>>toCollection(LinkedHashSet::new));
            } else {
                return ImmutableSortedSet.of(tierOid);
            }
        } else {
            return Collections.emptySet();
        }
    }

    /**
     * Compares the relative size of the two tiers, based on the number of coupons associated with
     * the tiers. If either of the tiers has a coupon value of zero, an {@link IncompatibleTiersException}
     * will be thrown.
     * @param tierOidA The first tier OID.
     * @param tierOidB The second tier OID.
     * @return A negative value, if tier one is smaller than tier two. Zero, if the tiers are equal. A positive
     * value if tier one is larger than tier two.
     * @throws IncompatibleTiersException Thrown if the tiers are not comparable. This may be due to either
     * tier having a coupon value of zero or the tiers being in different instance families.
     * @throws ComputeTierNotFoundException Thrown if either of the tiers can not be found in the
     * underlying cloud topology.
     */
    public long compareTiers(long tierOidA, long tierOidB) throws IncompatibleTiersException, ComputeTierNotFoundException {

        if (computeTierDataByOid.containsKey(tierOidA)
                && computeTierDataByOid.containsKey(tierOidB)) {

            final ComputeTierData computeTierDataA = computeTierDataByOid.get(tierOidA);
            final ComputeTierData computeTierDataB = computeTierDataByOid.get(tierOidB);

            if (computeTierDataA.hasCoverageFamily() && computeTierDataB.hasCoverageFamily()
                    && computeTierDataA.coverageFamily().equals(computeTierDataB.coverageFamily())) {
                return COMPUTE_TIER_COMPARATOR.compare(computeTierDataA, computeTierDataB);
            } else {
                throw new IncompatibleTiersException(
                        String.format("Unable to compare tiers: Tier A=%s, Tier B=%s",
                                computeTierDataA, computeTierDataB));
            }
        } else {
            final Set<Long> tiersNotFound = Stream.of(tierOidA, tierOidB)
                    .filter(Predicates.not(computeTierDataByOid::containsKey))
                    .collect(Collectors.toSet());

            throw new ComputeTierNotFoundException(
                    String.format("Unable to find compute tiers: %s", tiersNotFound));
        }
    }

    /**
     * Resolves and returns the coverage family for {@code tierOid}. If the normalization factor
     * (coupons) is not a positive value, the tier is assumed to not have a coverage family (the family
     * attribute under tier info will be ignored).
     * @param tierOid The target tier OID.
     * @return The coverage family, if one is found.
     */
    public Optional<String> getCoverageFamily(long tierOid) {
        return computeTierDataByOid.containsKey(tierOid)
                ? Optional.ofNullable(computeTierDataByOid.get(tierOid).coverageFamily())
                : Optional.empty();
    }

    /**
     * Resolves and returns the number of coupons for the {@code tierOid}.
     * @param tierOid The target tier OID.
     * @return The number of coupons, if the tier is found in the underlying cloud topology for this
     * resolver.
     */
    public Optional<Long> getNumCoupons(long tierOid) {
        return computeTierDataByOid.containsKey(tierOid)
                ? Optional.of(computeTierDataByOid.get(tierOid).numCoupons())
                : Optional.empty();
    }

    /**
     * A wrapper class for compute tier data related to family relationships.
     */
    @Value.Style(allParameters = true, defaults = @Value.Immutable(builder = false))
    @Immutable
    abstract static class ComputeTierData {

        public abstract long computeTierOid();

        @Nullable
        public abstract String family();

        public abstract long numCoupons();

        @Nullable
        @Value.Derived
        public String coverageFamily() {

            // If the compute tier does not have a non-zero number of coupons, there is no
            // size relationship between the tier and other tiers within the family. Therefore,
            // even if the tier has an associated family, we treat it as a standalone tier.
            if (numCoupons() > 0) {
                return family();
            } else {
                return null;
            }
        }

        @Value.Derived
        public boolean hasCoverageFamily() {
            return coverageFamily() != null;
        }
    }

    /**
     * A factory class for creating {@link ComputeTierFamilyResolver} instances.
     */
    public static class ComputeTierFamilyResolverFactory {

        /**
         * Creates a new {@link ComputeTierFamilyResolver} instance.
         * @param cloudTopology The {@link CloudTopology} instance, used to load and process all compute tier
         *                      instances within the topology.
         * @return The newly created {@link ComputeTierFamilyResolver} instance.
         */
        @Nonnull
        public ComputeTierFamilyResolver createResolver(@Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
            return new ComputeTierFamilyResolver(cloudTopology);
        }
    }

    /**
     * An exception indicating two tiers are not comparable.
     */
    public static final class IncompatibleTiersException extends Exception {

        /**
         * Constructs a new {@link IncompatibleTiersException} instance.
         * @param message The exception message.
         */
        public IncompatibleTiersException(@Nonnull String message) {
            super(message);
        }
    }

    /**
     * An exception indicating a tier could not be found.
     */
    public static final class ComputeTierNotFoundException extends Exception {

        /**
         * Constructs a new {@link ComputeTierNotFoundException} instance.
         * @param message The exception message.
         */
        public ComputeTierNotFoundException(@Nonnull String message) {
            super(message);
        }
    }

}
