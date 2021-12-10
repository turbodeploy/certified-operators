package com.vmturbo.cost.calculation.journal;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost.CostSourceLinkDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.trax.TraxNumber;

/**
 * Represents a cost, alongside the associated {@link CostSourceLink}.
 */
@Style(visibility = ImplementationVisibility.PACKAGE,
        overshadowImplementation = true,
        depluralize = true)
@Immutable
public interface CostItem {

    /**
     * The cost source link. This is the first link in a chain cost cost sources, in which a chain may
     * occur when multiple discounts are applied to a single rate (e.g. entity uptime and RI discounts).
     * @return The cost source link
     */
    @Nonnull
    CostSourceLink costSourceLink();

    /**
     * The cost of this time.
     * @return The cost of this item.
     */
    @Nonnull
    TraxNumber cost();

    /**
     * The commodity type.
     * @return The commodity type.
     */
    @Nonnull
    Optional<CommodityType> commodity();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link CostItem} instances.
     */
    class Builder extends ImmutableCostItem.Builder {}

    /**
     * A link in a chain of cost sources. The link may be the a part of a set of cost sources for a
     * specific category (e.g. on-demand rate <--- entity uptime discount <--- RI discount).
     */
    @Style(visibility = ImplementationVisibility.PACKAGE,
            allParameters = true,
            typeImmutable = "*Tuple")
    @Immutable
    interface CostSourceLink {

        /**
         * The {@link CostSource} of this link.
         * @return The {@link CostSource} of this link.
         */
        @Nonnull
        @Default
        default CostSource costSource() {
            return CostSource.UNCLASSIFIED;
        }

        /**
         * If this link represented a discount, {@link #discountedCostSourceLink()} represented the
         * discounted cost source this cost source was applied to.
         * @return The cost source this link was applied to, if this link represents a discount.
         */
        @Nonnull
        Optional<CostSourceLink> discountedCostSourceLink();

        /**
         * Follows the full chain of {@link CostSourceLink} instances, returning the set of {@link CostSource}
         * within the chain.
         * @return The set of {@link CostSource} within the chain.
         */
        @Lazy
        default Set<CostSource> costSourceChain() {
            return ImmutableSet.<CostSource>builder()
                    .add(costSource())
                    .addAll(discountedCostSourceLink()
                            .map(CostSourceLink::costSourceChain)
                            .orElse(Collections.emptySet()))
                    .build();
        }

        /**
         * Converts this {@link CostSourceLink} to a {@link CostSourceLinkDTO} protobuf message.
         * @return The {@link CostSourceLinkDTO} instance, presenting this {@link CostSourceLink} instance.
         */
        @Nonnull
        @Lazy
        default CostSourceLinkDTO toProtobuf() {

            final CostSourceLinkDTO.Builder dto = CostSourceLinkDTO.newBuilder()
                    .setCostSource(costSource());

            discountedCostSourceLink().ifPresent(discountLink ->
                    dto.setDiscountCostSourceLink(discountLink.toProtobuf()));

            return dto.build();
        }

        /**
         * Constructs a {@link CostSourceLink} from the optional {@code costSource}.
         * @param costSource The {@link CostSource} to use for this link. If empty, {@link CostSource#UNCLASSIFIED}
         *                   is used instead.
         * @return The newly constructed {@link CostSourceLink} instance.
         */
        @Nonnull
        static CostSourceLink of(@Nonnull Optional<CostSource> costSource) {
            return CostSourceLink.of(costSource.orElse(CostSource.UNCLASSIFIED), Optional.empty());
        }

        /**
         * Constructs a {@link CostSourceLink} from the {@code costSource}.
         * @param costSource The {@link CostSource} to use for this link.
         * @return The newly constructed {@link CostSourceLink} instance.
         */
        @Nonnull
        static CostSourceLink of(@Nonnull CostSource costSource) {
            return CostSourceLinkTuple.of(costSource, Optional.empty());
        }

        /**
         * Constructs a {@link CostSourceLink} from the {@code costSource} and optional
         * {@code discountedCostSource}.
         * @param costSource The {@link CostSource} to use for this link.
         * @param discountedCostSource The cost source this link is discounting. For example, a
         *                             {@link CostSource#ENTITY_UPTIME_DISCOUNT} may be applied to a
         *                             {@link CostSource#ON_DEMAND_RATE}. In that case, the {@link CostSource#ON_DEMAND_RATE}
         *                             would represent the discounted cost source.
         * @return The newly constructed {@link CostSourceLink} instance.
         */
        @Nonnull
        static CostSourceLink of(@Nonnull CostSource costSource,
                                  @Nonnull Optional<CostSourceLink> discountedCostSource) {
            return CostSourceLinkTuple.of(costSource, discountedCostSource);
        }
    }
}
