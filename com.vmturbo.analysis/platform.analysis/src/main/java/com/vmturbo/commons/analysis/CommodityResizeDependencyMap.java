package com.vmturbo.commons.analysis;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * The commodity resize dependency map is a map from Sold Commodity to dependent Bought
 * Commodities, with their resize dependency functions. Each commodity has two functions,
 * one to be used when the sold commodity is resized up and the other when it is resized
 * down.
 */
public class CommodityResizeDependencyMap {

    private static final Map<UpdateFunction, UpdatingFunctionTO> updateFunctionMapping =
            new ImmutableMap.Builder<UpdateFunction, UpdatingFunctionTO>()
                .put(UpdateFunction.AVG_ADD, UpdatingFunctionTO.newBuilder()
                    .setAvgAdd(UpdatingFunctionTO.Average.getDefaultInstance()).build())
                .put(UpdateFunction.MAX, UpdatingFunctionTO.newBuilder()
                    .setMax(UpdatingFunctionTO.Max.getDefaultInstance()).build())
                .put(UpdateFunction.MIN, UpdatingFunctionTO.newBuilder()
                    .setMin(UpdatingFunctionTO.Min.getDefaultInstance()).build())
                .put(UpdateFunction.PROJECT_SECOND, UpdatingFunctionTO.newBuilder()
                    .setProjectSecond(UpdatingFunctionTO.ProjectSecond.getDefaultInstance()).build())
                .put(UpdateFunction.DELTA, UpdatingFunctionTO.newBuilder()
                    .setDelta(UpdatingFunctionTO.Delta.getDefaultInstance()).build())
                .put(UpdateFunction.IGNORE_CONSUMPTION, UpdatingFunctionTO.newBuilder()
                    .setIgnoreConsumption(UpdatingFunctionTO.IgnoreConsumption.getDefaultInstance()).build())
                .put(UpdateFunction.UPDATE_EXPENSES, UpdatingFunctionTO.newBuilder()
                    .setUpdateExpenses(UpdatingFunctionTO.UpdateExpenses.getDefaultInstance()).build())
                .put(UpdateFunction.UPDATE_COUPON, UpdatingFunctionTO.newBuilder()
                                .setUpdateCoupon(UpdatingFunctionTO.UpdateCoupon.getDefaultInstance()).build())
                .build();

    /**
     * Convert {@link UpdateFunction} to {@link UpdatingFunctionTO}.
     *
     * @param function and increment or decrement update function represented as
     * {@link UpdateFunction}
     * @return the equivalent representation as {@link UpdatingFunctionTO}
     */
    public static UpdatingFunctionTO updatingFunctionTO(UpdateFunction function) {
        return updateFunctionMapping.get(function);
    }

    /**
     * Commodity resize specification includes a commodity type (numerical value)
     * and the increment and decrement functions.
     */
    public static class CommodityResizeDependencySpec {
        private final int commodityType;
        private final UpdateFunction decrement;
        private final UpdateFunction increment;

        public CommodityResizeDependencySpec(int commodityType,
                UpdateFunction increment, UpdateFunction decrement) {
            this.commodityType = commodityType;
            this.increment = increment;
            this.decrement = decrement;
        }

        public int getCommodityType() {
            return commodityType;
        }

        public UpdateFunction getIncrementFunction() {
            return increment;
        }

        public UpdateFunction getDecrementFunction() {
            return decrement;
        }
    }

    /**
     * Mapping from commodity numerical type to the commodity resize dependency spec.
     */
    public static final Map<Integer, List<CommodityResizeDependencySpec>>
            commodityResizeDependencyMap =
            new ImmutableMap.Builder<Integer, List<CommodityResizeDependencySpec>>()
                    .put(CommodityType.VMEM_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.MEM_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN),
                            new CommodityResizeDependencySpec(CommodityType.MEM_PROVISIONED_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.PROJECT_SECOND),
                            new CommodityResizeDependencySpec(CommodityType.VMEM_LIMIT_QUOTA_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.VMEM_REQUEST_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.VMEM_REQUEST_QUOTA_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN),
                            new CommodityResizeDependencySpec(CommodityType.VMEM_REQUEST_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.VCPU_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.CPU_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN),
                            new CommodityResizeDependencySpec(CommodityType.CPU_PROVISIONED_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.PROJECT_SECOND),
                            new CommodityResizeDependencySpec(CommodityType.VCPU_LIMIT_QUOTA_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.VCPU_REQUEST_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.VCPU_REQUEST_QUOTA_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN),
                            new CommodityResizeDependencySpec(CommodityType.VCPU_REQUEST_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.VSTORAGE_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.STORAGE_AMOUNT_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN),
                            new CommodityResizeDependencySpec(CommodityType.STORAGE_PROVISIONED_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.MEM_ALLOCATION_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.MEM_ALLOCATION_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.CPU_ALLOCATION_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.CPU_ALLOCATION_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.STORAGE_ALLOCATION_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.STORAGE_ALLOCATION_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.DB_MEM_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.VMEM_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .put(CommodityType.HEAP_VALUE, ImmutableList.of(
                            new CommodityResizeDependencySpec(CommodityType.VMEM_VALUE,
                                    UpdateFunction.DELTA, UpdateFunction.MIN)
                            )
                    )
                    .build();

    /**
     * Mapping from commodity numerical type to the commodity resize co-dependency spec.
     * Eg, For resize DOWN: When vMem scales down, we need to make sure that the resized capacity satisfies the sum
     * of the capacities of the heap/dbMem capacities sold by the consumer app/database
     * For resize UP: When heap/dbMem resizes up, we need to make sure that the underlying provider's vMem has
     * enough capacity to support the sum of the capacities of the customers.
     */
    public static final Map<Integer, List<Integer>>
            commodityResizeProducesMap =
            new ImmutableMap.Builder<Integer, List<Integer>>()
                    .put(CommodityType.VMEM_VALUE,
                            ImmutableList.of(CommodityType.HEAP_VALUE, CommodityType.DB_MEM_VALUE))
                    .build();
}