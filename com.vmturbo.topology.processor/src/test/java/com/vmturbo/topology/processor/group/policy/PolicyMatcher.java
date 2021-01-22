package com.vmturbo.topology.processor.group.policy;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.policy.application.InvalidMergePolicyTypeException;
import com.vmturbo.topology.processor.group.policy.application.MergePolicy;

/**
 * A helper for constructing matchers that make assertions about the behavior of policies.
 */
public class PolicyMatcher {

    private final TopologyGraph<TopologyEntity> topologyGraph;

    /**
     * Prevent construction.
     */
    public PolicyMatcher(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        this.topologyGraph = topologyGraph;
    }

    public Matcher<TopologyEntity> hasProviderSegment(final long segmentId) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                    .anyMatch(commodity ->
                        commodity.getCommodityType().getType() == CommodityType.SEGMENTATION_VALUE &&
                            commodity.getCommodityType().getKey().equals(Long.toString(segmentId)));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("should be selling a segmentation commodity.");
            }
        };
    }

    public Matcher<TopologyEntity> hasProviderSegmentWithCapacity(final long segmentId,
                                                          final double expectedCapacity) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                    .anyMatch(commodity ->
                        commodity.getCommodityType().getType() == CommodityType.SEGMENTATION_VALUE &&
                            commodity.getCommodityType().getKey().equals(Long.toString(segmentId)) &&
                            matchesCapacity(commodity, expectedCapacity, 0.2D)
                    );
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("should be selling a segmentation commodity with approximate capacity of "
                    + expectedCapacity + ".");
            }

            /**
             * Check that the commodity has an expected capacity within an epsilon factor.
             *
             * @param commodity The commodity to check.
             * @param expectedCapacity The expected capacity for the commodity.
             * @param epsilon The epsilon factor that constitutes a match.
             * @return If the commodity capacity matches the expected capacity.
             */
            private boolean matchesCapacity(@Nonnull final CommoditySoldDTO commodity,
                                            final double expectedCapacity,
                                            final double epsilon) {

                if (!commodity.hasCapacity()) {
                    return false;
                }
                if (Double.isFinite(commodity.getCapacity())) {
                    return Math.abs(commodity.getCapacity() - expectedCapacity) < epsilon;
                } else {
                    return commodity.getCapacity() == expectedCapacity;
                }
            }
        };
    }

    public Matcher<TopologyEntity> hasProviderSegmentWithCapacityAndUsed(final long segmentId,
                                                                 final float expectedCapacity,
                                                                 final float expectedUsed) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                    .anyMatch(commodity ->
                        commodity.getCommodityType().getType() == CommodityType.SEGMENTATION_VALUE &&
                            commodity.getCommodityType().getKey().equals(Long.toString(segmentId)) &&
                            matchesCapacity(commodity, expectedCapacity, 0.2f) &&
                            matchesUsed(commodity, expectedUsed, 0.2f)
                    );
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("should be selling a segmentation commodity with approximate capacity of "
                    + expectedCapacity + " and used value " + expectedUsed + ".");
            }

            /**
             * Check that the commodity has an expected capacity within an epsilon factor.
             *
             * @param commodity The commodity to check.
             * @param expectedCapacity The expected capacity for the commodity.
             * @param epsilon The epsilon factor that constitutes a match.
             * @return If the commodity capacity matches the expected capacity.
             */
            private boolean matchesCapacity(@Nonnull final CommoditySoldDTO commodity,
                                            final float expectedCapacity,
                                            final float epsilon) {

                if (!commodity.hasCapacity()) {
                    return false;
                }
                if (Double.isFinite(commodity.getCapacity())) {
                    return Math.abs(commodity.getCapacity() - expectedCapacity) < epsilon;
                } else {
                    return commodity.getCapacity() == expectedCapacity;
                }
            }

            private boolean matchesUsed(@Nonnull final CommoditySoldDTO commodity,
                                        final float expectedUsed,
                                        final float epsilon) {
                if (!commodity.hasUsed()) {
                    return false;
                }
                if (Double.isFinite(commodity.getUsed())) {
                    return Math.abs(commodity.getUsed() - expectedUsed) < epsilon;
                } else {
                    return commodity.getUsed() == expectedUsed;
                }
            }
        };
    }

    public Matcher<TopologyEntity> hasConsumerSegment(long segmentId, final EntityType expectedEntityType) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream()
                    .anyMatch(commodityBoughtGroup -> commodityBoughtGroup.getCommodityBoughtList().stream()
                        .anyMatch(commodity ->
                            commodity.getCommodityType().getType() == CommodityType.SEGMENTATION_VALUE &&
                                (!commodityBoughtGroup.hasProviderId() ||
                                    (commodityBoughtGroup.hasProviderId() &&
                                        providerIsExpectedEntityType(commodityBoughtGroup.getProviderId())))
                                && commodity.getCommodityType().getKey().equals(Long.toString(segmentId))));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("should be buying a segmentation commodity from an entity of type "
                    + expectedEntityType + ".");
            }

            private boolean providerIsExpectedEntityType(final long providerId) {
                return topologyGraph.getEntity(providerId).get()
                    .getEntityType() == expectedEntityType.getNumber();
            }
        };
    }

    public Matcher<TopologyEntity> hasConsumerSegment(long segmentId, long providerId, long volumeId) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream()
                    .anyMatch(commodityBoughtGroup -> commodityBoughtGroup.getCommodityBoughtList().stream()
                        .anyMatch(commodity ->
                            commodity.getCommodityType().getType() == CommodityType.SEGMENTATION_VALUE &&
                                (!commodityBoughtGroup.hasProviderId() ||
                                    commodityBoughtGroup.getProviderId() == providerId)
                                && commodity.getCommodityType().getKey().equals(Long.toString(segmentId))
                                && commodityBoughtGroup.getVolumeId() == volumeId));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("should be buying a segmentation commodity from provider: "
                    + providerId + " with related volume: " + volumeId + ".");
            }
        };
    }

    /**
     * Check if the key of the 'sold' cluster commodity is set to the policy OID and values are matches.
     *
     * @param policy merge policy.
     * @param value extra value to check.
     * @return true if key is set to policy OID and the extra property match (if it exists)
     */
    public Matcher<TopologyEntity> hasCommoditySoldClusterType(@Nonnull final MergePolicy policy, Double value) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                        .anyMatch(commodity -> {
                                    boolean isTypeAndKeyRight = commodity.getCommodityType().getType() == getCommodityType(policy)
                                            && commodity.getCommodityType().getKey().equals(Long.toString(policy.getPolicyDefinition().getId()));
                                    if (value != null) {
                                        return isTypeAndKeyRight && commodity.getCapacity() == value;
                                    }
                                    return isTypeAndKeyRight;

                                }
                        );
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Should has Cluster commodity type in the commoditySoldList" +
                        " with Key equal to Policy OID.");
            }
        };
    }

    /**
     * Check if specific type are in the commodity sold list.
     *
     * @param type type to be check
     * @return true if the type exists.
     */
    public Matcher<TopologyEntity> hasCommoditySoldType(final int type) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                        .anyMatch(commodity ->
                                    commodity.getCommodityType().getType() == type
                        );
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Should has commodity type in the commoditySoldList: " + type);
            }
        };
    }

    /**
     * Check if the key of the 'bought' cluster commodity is set to the policy OID.
     *
     * @param policy merge policy OID.
     * @return true if key is set to policy OID.
     */
    public Matcher<TopologyEntity>  hasCommodityBoughtClusterType(final MergePolicy policy) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream()
                        .anyMatch(commodityBoughtGroup -> commodityBoughtGroup.getCommodityBoughtList().stream()
                                .anyMatch(commodity ->
                                        commodity.getCommodityType().getType() == getCommodityType(policy) &&
                                                commodity
                                                        .getCommodityType()
                                                        .getKey()
                                                        .equals(Long.toString(policy.getPolicyDefinition().getId()))));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Should have Cluster commodity type in CommodityBoughtList with Key equal to policy OID");
            }

        };
    }

    /**
     *
     * Check if the key of the 'bought' cluster commodity is set to the policy OID and types are matches.
     *
     * @param policyOid merge policy OID.
     * @param type  extra value to check.
     * @return true if key is set to policy OID and type matches.
     *
     */
    public Matcher<TopologyEntity> hasCommodityBoughtClusterType(final long policyOid, final int type) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream()
                        .anyMatch(commodityBoughtGroup -> commodityBoughtGroup.getCommodityBoughtList().stream()
                                .anyMatch(commodity ->
                                        commodity.getCommodityType().getType() == type &&
                                                commodity.getCommodityType().getKey().equals(Long.toString(policyOid))));
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("Should has Cluster commodity type in the commodityBoughtList" +
                        " with Key equal to Policy OID.");
            }
        };
    }

    public static SearchParametersCollection searchParametersCollection() {
        return SearchParametersCollection.newBuilder()
            .addSearchParameters(SearchParameters.getDefaultInstance())
            .build();
    }

    public static StaticGroupMembers staticGroupMembers(final Long... oids) {
        return StaticGroupMembers.newBuilder()
            .addAllStaticMemberOids(Arrays.asList(oids))
            .build();
    }

    /**
     * Get the commodity type based on the current merge policy type
     * @return
     */
    public static int getCommodityType(MergePolicy policy) {
        final PolicyInfo.MergePolicy mergePolicy =
                policy.getPolicyDefinition().getPolicyInfo().getMerge();
        switch (mergePolicy.getMergeType()) {
            case CLUSTER:
                return CommodityType.CLUSTER_VALUE;
            case STORAGE_CLUSTER:
                return CommodityType.STORAGE_CLUSTER_VALUE;
            case DATACENTER:
                return CommodityType.DATACENTER_VALUE;
            case DESKTOP_POOL:
                return CommodityType.ACTIVE_SESSIONS_VALUE;
            default:
                throw new InvalidMergePolicyTypeException("Invalid merge policy type: "
                        + mergePolicy.getMergeType());
        }
    }

}
