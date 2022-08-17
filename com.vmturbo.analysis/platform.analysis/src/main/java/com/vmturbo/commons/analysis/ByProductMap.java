package com.vmturbo.commons.analysis;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Map from the resizing commodity to the byProduct commodity.
 */
public class ByProductMap {

    private ByProductMap() {}

    /**
     * Mapping between a resizing commodity to a list of byProduct commodities.
     */
    public static final Map<Integer, List<ByProductInfo>> byProductMap =
        new ImmutableMap.Builder<Integer, List<ByProductInfo>>().put(CommodityType.VCPU_VALUE,
                ImmutableList.of(new ByProductInfo(CommodityType.VCPU_THROTTLING_VALUE,
                       UpdatingFunctionDTOs.UpdatingFunctionTO.newBuilder()
                               .setInverseSquare(UpdatingFunctionDTOs.UpdatingFunctionTO
                                       .InverseSquare.newBuilder().build()).build()))).build();

    /**
     * This caches the combination of byProduct and projectionFunctions.
     */
    @Immutable
    public static class ByProductInfo {
        private final Integer byProduct;
        private final UpdatingFunctionDTOs.UpdatingFunctionTO projectionFunction;

        /**
         * Constructs the {@link ByProductInfo} object.
         *
         * @param byProduct is the base type of the by product commodity.
         * @param projectionFunction is the corresponding projection function for the by-product commodity.
         */
        ByProductInfo(final Integer byProduct, final UpdatingFunctionDTOs.UpdatingFunctionTO projectionFunction) {
            this.byProduct = byProduct;
            this.projectionFunction = projectionFunction;
        }

        public Integer getByProduct() {
            return byProduct;
        }

        public UpdatingFunctionDTOs.UpdatingFunctionTO getProjectionFunction() {
            return projectionFunction;
        }

        /**
         * Get a builder for the by product.
         *
         * @param byProduct The by product list for the by products builder.
         * @param projectionFunction is the corresponding projection function for the by-product commodity.
         * @return a builder for the by products.
         */
        public static Builder newBuilder(@Nonnull final Integer byProduct,
                                         @Nonnull final UpdatingFunctionDTOs.UpdatingFunctionTO projectionFunction) {
            return new Builder(byProduct, projectionFunction);
        }

        /**
         * A builder for by products.
         */
        public static class Builder {
            private final Integer byProduct;
            private final UpdatingFunctionDTOs.UpdatingFunctionTO projectionFunction;

            /**
             * Create a new builder.
             * @param byProduct The by product for the by product builder.
             * @param projectionFunction The projection function for the product builder.
             */
            private Builder(@Nonnull final Integer byProduct, @Nonnull final UpdatingFunctionDTOs.UpdatingFunctionTO projectionFunction) {
                this.byProduct = Objects.requireNonNull(byProduct);
                this.projectionFunction = Objects.requireNonNull(projectionFunction);
            }

            /**
             * Build the by products.
             *
             * @return the built by products.
             */
            public ByProductInfo build() {
                return new ByProductInfo(byProduct, projectionFunction);
            }
        }
    }
}
