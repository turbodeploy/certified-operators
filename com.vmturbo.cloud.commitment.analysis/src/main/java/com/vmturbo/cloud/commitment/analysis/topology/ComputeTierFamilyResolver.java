package com.vmturbo.cloud.commitment.analysis.topology;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
                                () -> new TreeSet<>(Comparator.comparing(ComputeTierData::numCoupons)
                                        .thenComparing(ComputeTierData::computeTierOid)))));
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
}
