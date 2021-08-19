package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * Provides a mapping of {@link ReservedInstanceSpecMatcher} and {@link ReservedInstanceInventoryMatcher}
 * instances, based on a target region. The instances are loaded per region, in order to avoid loading
 * unnecessary RI sepcs/RI inventory and in order to limit the memory consumption on creation of the
 * instances (given each does some normalization on the data).
 *
 * <p>This cache is meant to only have a lifecycle of a single round of analysis, given the RI and
 * RI specs will be based on the demand clusters and purchasing constraints of the analysis.
 */
public class RegionalRIMatcherCache {

    private final Map<Long, ReservedInstanceSpecMatcher> riSpecMatchersByRegionOid = new HashMap<>();

    private final Map<Long, ReservedInstanceInventoryMatcher> riInventoryMatchersByRegionOid = new HashMap<>();

    private final RISpecMatcherFactory riSpecMatcherFactory;

    private final ReservedInstanceInventoryMatcherFactory riInventoryMatcherFactory;

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final ComputeTierFamilyResolver computeTierFamilyResolver;

    private final TopologyInfo topologyInfo;


    public RegionalRIMatcherCache(@Nonnull RISpecMatcherFactory riSpecMatcherFactory,
                                  @Nonnull ReservedInstanceInventoryMatcherFactory riInventoryMatcherFactory,
                                  @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                  @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                  @Nonnull TopologyInfo topologyInfo) {

        this.riSpecMatcherFactory = Objects.requireNonNull(riSpecMatcherFactory);
        this.riInventoryMatcherFactory = Objects.requireNonNull(riInventoryMatcherFactory);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.computeTierFamilyResolver = Objects.requireNonNull(computeTierFamilyResolver);
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
    }


    @Nonnull
    public ReservedInstanceSpecMatcher getOrCreateRISpecMatchForRegion(long regionOid) {
        return riSpecMatchersByRegionOid.computeIfAbsent(regionOid,
                (__) -> riSpecMatcherFactory.createRegionalMatcher(
                        computeTierFamilyResolver,
                        regionOid));
    }

    @Nonnull
    public ReservedInstanceInventoryMatcher getOrCreateRIInventoryMatcherForRegion(long regionOid) {
        return riInventoryMatchersByRegionOid.computeIfAbsent(regionOid,
                (__) -> riInventoryMatcherFactory.createRegionalMatcher(
                        cloudTopology,
                        getOrCreateRISpecMatchForRegion(regionOid),
                        topologyInfo, regionOid));
    }

    @Nonnull
    public Map<Long, ReservedInstanceSpecMatcher> getAllRISpecMatchersByRegionOid() {
        return Collections.unmodifiableMap(riSpecMatchersByRegionOid);
    }

    @Nonnull
    public Map<Long, ReservedInstanceInventoryMatcher> getAllRIInventoryMatchersByRegionOid() {
        return Collections.unmodifiableMap(riInventoryMatchersByRegionOid);
    }


}
