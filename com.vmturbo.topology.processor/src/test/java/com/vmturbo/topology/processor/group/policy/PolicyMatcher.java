package com.vmturbo.topology.processor.group.policy;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.topology.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * A helper for constructing matchers that make assertions about the behavior of policies.
 */
public class PolicyMatcher {

    private final TopologyGraph topologyGraph;

    /**
     * Prevent construction.
     */
    public PolicyMatcher(@Nonnull final TopologyGraph topologyGraph) {
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
                                                          final float expectedCapacity) {
        return new BaseMatcher<TopologyEntity>() {
            @Override
            public boolean matches(Object o) {
                final TopologyEntity entity = (TopologyEntity)o;
                return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
                    .anyMatch(commodity ->
                        commodity.getCommodityType().getType() == CommodityType.SEGMENTATION_VALUE &&
                            commodity.getCommodityType().getKey().equals(Long.toString(segmentId)) &&
                            matchesCapacity(commodity, expectedCapacity, 0.2f)
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
}
