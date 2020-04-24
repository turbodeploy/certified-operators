package com.vmturbo.market.topology.conversions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.Constant;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A final class that defines constants and price functions
 * to be used by market topology creation and market analysis.
 * Initially this class was in operations manager repository.
 * It was created here to keep the logic of fetching correct
 * price function in one place and not in two different repositories.
 * @author nitya
 *
 */
public final class MarketAnalysisUtils {

    // Epsilon for float comparison.
    public static final float EPSILON = 0.00001f;

    // flow-0 key string constant used to decide ncm price function
    public static final String FLOW_ZERO_KEY = "FLOW-0";

    // flow-1 key string constant used to decide ncm price function
    public static final String FLOW_ONE_KEY = "FLOW-1";

    // flow-2 key string constant used to decide ncm price function
    public static final String FLOW_TWO_KEY = "FLOW-2";

    // default quote factor used in market analysis
    public static final float QUOTE_FACTOR = 0.68f;

    // default move cost factor used in market analysis
    public static final float LIVE_MARKET_MOVE_COST_FACTOR = 0.05f;

    private MarketAnalysisUtils() {}

    /**
     * Constant price function used for these commodities.
     */
    private static final Set<Integer> CONSTANT_PRICE_TYPES = ImmutableSet.of(
                    CommodityDTO.CommodityType.COOLING_VALUE,
                    CommodityDTO.CommodityType.POWER_VALUE,
                    CommodityDTO.CommodityType.SPACE_VALUE,
                    CommodityDTO.CommodityType.APPLICATION_VALUE,
                    CommodityDTO.CommodityType.CLUSTER_VALUE,
                    CommodityDTO.CommodityType.DATACENTER_VALUE,
                    CommodityDTO.CommodityType.DATASTORE_VALUE,
                    CommodityDTO.CommodityType.DSPM_ACCESS_VALUE,
                    CommodityDTO.CommodityType.NETWORK_VALUE,
                    CommodityDTO.CommodityType.SEGMENTATION_VALUE,
                    CommodityDTO.CommodityType.DRS_SEGMENTATION_VALUE,
                    CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE,
                    CommodityDTO.CommodityType.VAPP_ACCESS_VALUE,
                    CommodityDTO.CommodityType.VDC_VALUE,
                    CommodityDTO.CommodityType.VMPM_ACCESS_VALUE,
                    CommodityDTO.CommodityType.EXTENT_VALUE,
                    CommodityDTO.CommodityType.ACTIVE_SESSIONS_VALUE);

    /**
     * Step price function used for these commodities.
     */
    private static final Set<Integer> STEP_PRICE_TYPES =
                    ImmutableSet.of(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                                    CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE,
                                    CommodityDTO.CommodityType.VSTORAGE_VALUE,
                                    CommodityDTO.CommodityType.VCPU_REQUEST_VALUE,
                                    CommodityDTO.CommodityType.VMEM_REQUEST_VALUE);

    /**
     * Finite standard weighted price function is used for these commodities.
     */
    private static final Set<Integer> FINITE_SWP_PRICE_TYPES =
                    ImmutableSet.of(CommodityDTO.CommodityType.Q1_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q2_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q3_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q4_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q5_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q6_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q7_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q8_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q16_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q32_VCPU_VALUE,
                                    CommodityDTO.CommodityType.Q64_VCPU_VALUE,
                                    CommodityDTO.CommodityType.QN_VCPU_VALUE);

    /**
     * Commodities that will be priced with SquaredReciprocalBought function.
     */
    private static final Set<Integer> IMAGE_COMMODITY_SET =
                    ImmutableSet.of(CommodityDTO.CommodityType.IMAGE_MEM_VALUE,
                                    CommodityDTO.CommodityType.IMAGE_CPU_VALUE,
                                    CommodityDTO.CommodityType.IMAGE_STORAGE_VALUE);

    /**
     * If an entity buys from guaranteed sellers then it is a guaranteed buyer.
     */
    public static final Set<Integer> GUARANTEED_SELLER_TYPES =
                    ImmutableSet.of(EntityType.STORAGE_VALUE, EntityType.PHYSICAL_MACHINE_VALUE);

    /**
     * Set of entities that are by default allowed to provision in market analysis.
     */
    public static final Set<Integer> CLONABLE_TYPES =
                    ImmutableSet.of(EntityType.PHYSICAL_MACHINE_VALUE, EntityType.STORAGE_VALUE,
                                    EntityType.STORAGE_CONTROLLER_VALUE, EntityType.CHASSIS_VALUE,
                                    EntityType.DISK_ARRAY_VALUE);

    /**
     * Flow commodities set for identifying flow commodities used in NCM.
     */
    public static final Set<Integer> FLOW_COMMODITY_SET =
                    ImmutableSet.of(CommodityDTO.CommodityType.FLOW_VALUE);

    /**
     * Commodities for which used is capped to capacity value (approximately 0.99 * capacity)
     * whenever we have used > capacity.
     */
    public static final Set<Integer> COMMODITIES_TO_CAP =
                    ImmutableSet.of(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE);

    /**
     * Same as COMMODITIES_TO_CAP above but it's valid for these commodities to have used > capacity,
     * e.g. for those commodities with fake capacity.
     */
    public static final Set<Integer> VALID_COMMODITIES_TO_CAP =
                    ImmutableSet.of(CommodityDTO.CommodityType.RESPONSE_TIME_VALUE,
                                    CommodityDTO.CommodityType.TRANSACTION_VALUE,
                                    CommodityDTO.CommodityType.SLA_COMMODITY_VALUE);

    /**
     * Commodities that are not capped but skipped for used > capacity check. They are sent
     * with used > capacity to market and error log is not printed for such commodities
     * as market can handle them.
     */
    public static final Set<Integer> COMMODITIES_TO_SKIP = ImmutableSet.of(
                    CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE,
                    CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE,
                    CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE,
                    CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
                    CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE,
                    CommodityDTO.CommodityType.VCPU_VALUE,
                    CommodityDTO.CommodityType.VMEM_VALUE,
                    CommodityDTO.CommodityType.CPU_VALUE,
                    CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE,
                    CommodityDTO.CommodityType.Q1_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q2_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q3_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q4_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q5_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q6_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q7_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q8_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q16_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q32_VCPU_VALUE,
                    CommodityDTO.CommodityType.Q64_VCPU_VALUE,
                    CommodityDTO.CommodityType.QN_VCPU_VALUE,
                    CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE);

    /**
     * Map of list of commodities that simulation of resize action based on historical value
     * should be skipped by commodity type.
     */
    public static final Map<Integer, List<Integer>> HISTORY_BASED_RESIZE_DEPENDENCY_SKIP_MAP =
        ImmutableMap.<Integer, List<Integer>>builder()
            .put(CommodityDTO.CommodityType.VMEM_VALUE, Collections.singletonList(CommodityDTO.CommodityType.MEM_VALUE))
            .put(CommodityDTO.CommodityType.VCPU_VALUE, Collections.singletonList(CommodityDTO.CommodityType.CPU_VALUE))
            .build();

    /**
     * These are the types that in the platform are subclasses of AccessCommodity.
     * TODO: Make this check part of {@link CommodityDTO.CommodityType}.
     */
    public static final Set<Integer> ACCESS_COMMODITY_TYPES =
                    ImmutableSet.of(CommodityDTO.CommodityType.APPLICATION_VALUE,
                                    CommodityDTO.CommodityType.CLUSTER_VALUE,
                                    CommodityDTO.CommodityType.DATACENTER_VALUE,
                                    CommodityDTO.CommodityType.DATASTORE_VALUE,
                                    CommodityDTO.CommodityType.DISK_ARRAY_ACCESS_VALUE,
                                    CommodityDTO.CommodityType.DSPM_ACCESS_VALUE,
                                    CommodityDTO.CommodityType.NETWORK_VALUE,
                                    CommodityDTO.CommodityType.PROCESSING_UNITS_VALUE,
                                    CommodityDTO.CommodityType.SEGMENTATION_VALUE,
                                    CommodityDTO.CommodityType.DRS_SEGMENTATION_VALUE,
                                    CommodityDTO.CommodityType.SERVICE_LEVEL_CLUSTER_VALUE,
                                    CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE,
                                    CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE,
                                    CommodityDTO.CommodityType.TENANCY_ACCESS_VALUE,
                                    CommodityDTO.CommodityType.VAPP_ACCESS_VALUE,
                                    CommodityDTO.CommodityType.VDC_VALUE,
                                    CommodityDTO.CommodityType.VMPM_ACCESS_VALUE,
                                    CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE);

    public static final Set<Integer> SKIPPED_ENTITY_TYPES =
                    ImmutableSet.of(EntityType.ACTION_MANAGER_VALUE);

    public static final Set<Integer> PROVISIONED_COMMODITIES =
                    ImmutableSet.of(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE,
                                    CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE,
                                    CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE);

    public static final Set<Integer> CLONE_COMMODITIES_WITH_NEW_TYPE =
                    ImmutableSet.of(CommodityDTO.CommodityType.APPLICATION_VALUE);

    public static final Set<Integer> GUARANTEED_BUYER_TYPES =
                    ImmutableSet.of(EntityType.VIRTUAL_DATACENTER_VALUE, EntityType.VPOD_VALUE,
                                    EntityType.DPOD_VALUE);

    public static final Set<Integer> VDC_COMMODITY_TYPES =
                    ImmutableSet.of(CommodityDTO.CommodityType.CPU_ALLOCATION_VALUE,
                                    CommodityDTO.CommodityType.MEM_ALLOCATION_VALUE,
                                    CommodityDTO.CommodityType.CPU_REQUEST_ALLOCATION_VALUE,
                                    CommodityDTO.CommodityType.MEM_REQUEST_ALLOCATION_VALUE,
                                    CommodityDTO.CommodityType.STORAGE_ALLOCATION_VALUE,
                                    CommodityDTO.CommodityType.FLOW_ALLOCATION_VALUE);

    /**
     * Commodities set for which consumption is ignored during move in market.
     */
    public static final Set<Integer> IGNORE_UPDATE_TYPES =
                    ImmutableSet.of(CommodityDTO.CommodityType.INSTANCE_DISK_SIZE_VALUE,
                                    CommodityDTO.CommodityType.INSTANCE_DISK_TYPE_VALUE,
                                    CommodityDTO.CommodityType.IMAGE_CPU_VALUE,
                                    CommodityDTO.CommodityType.IMAGE_MEM_VALUE,
                                    CommodityDTO.CommodityType.IMAGE_STORAGE_VALUE);

    /**
     * Overhead calculated in market analysis for these commodities.
     */
    public static final Set<Integer> COMM_TYPES_TO_ALLOW_OVERHEAD = ImmutableSet
                    .of(CommodityDTO.CommodityType.CPU_VALUE, CommodityDTO.CommodityType.MEM_VALUE);

    /**
     *  Updating functions are used when taking actions in computing the new
     *  used and peak used values of commodities sold. Below are reusable DTOs
     *  used when constructing commodity sold settings.
     */
    private static final UpdatingFunctionTO AVERAGE_UPDATING_FUNCTION = UpdatingFunctionTO.newBuilder()
                    .setAvgAdd(UpdatingFunctionTO.Average.getDefaultInstance()).build();

    @SuppressWarnings("unused")
    private static final UpdatingFunctionTO DELTA_UPDATING_FUNCTION = UpdatingFunctionTO.newBuilder()
                    .setDelta(UpdatingFunctionTO.Delta.getDefaultInstance()).build();

    private static final UpdatingFunctionTO EXTERNAL_UPDATING_FUNCTION = UpdatingFunctionTO.newBuilder()
                    .setExternalUpdate(
                                    UpdatingFunctionTO.ExternalUpdateFunction.getDefaultInstance())
                    .build();

    private static final UpdatingFunctionTO PROJECTED_SECOND_UPDATING_FUNCTION = UpdatingFunctionTO
                    .newBuilder()
                    .setProjectSecond(UpdatingFunctionTO.ProjectSecond.getDefaultInstance())
                    .build();

    /**
     * Updating function that ignores consumption from consumer when it places
     * on provider in market. This way we do not update provider at all.
     * This updating function is used by default in cloud and for on-prem,
     * we use it in VDI feature
     */
    private static final UpdatingFunctionTO IGNORE_CONSUMPTION_UPDATING_FUNCTION = UpdatingFunctionTO
                    .newBuilder()
                    .setIgnoreConsumption(UpdatingFunctionTO.IgnoreConsumption.getDefaultInstance())
                    .build();

    private static final PriceFunctionTO CONSTANT = PriceFunctionTO.newBuilder()
                    .setConstant(PriceFunctionTO.Constant.newBuilder().setValue(1.0f).build())
                    .build();

    private static final PriceFunctionTO STEP = PriceFunctionTO.newBuilder()
                    .setStep(PriceFunctionTO.Step.newBuilder().setStepAt(1)
                                    .setPriceAbove(Float.POSITIVE_INFINITY).setPriceBelow(0.0001f)
                                    .build())
                    .build();

    private static final PriceFunctionTO SWP = PriceFunctionTO.newBuilder().setStandardWeighted(
                    PriceFunctionTO.StandardWeighted.newBuilder().setWeight(1.0f).build()).build();

    private static final PriceFunctionTO FSWP = PriceFunctionTO.newBuilder()
                    .setFiniteStandardWeighted(PriceFunctionTO.FiniteStandardWeighted.newBuilder()
                                    .setWeight(1.0f).build())
                    .build();

    private static final PriceFunctionTO IG = PriceFunctionTO.newBuilder()
                    .setIgnoreUtilization(PriceFunctionTO.IgnoreUtilization.newBuilder())
                    .build();

    /**
     * Squared reciprocal price function used for VDI on-prem.
     * https://vmturbo.atlassian.net/wiki/spaces/Home/pages/876347519/Price+function+based+on+excess+capacity
     */
    private static final PriceFunctionTO SQRP = PriceFunctionTO.newBuilder()
                    .setSquaredReciprocalBought(
                                    PriceFunctionTO.SquaredReciprocalBought.getDefaultInstance())
                    .build();

    /**
     * External price function for calculation of flow outside of the market.
     * This price function is only used for NCM feature. It tries to calculate
     * flow prices based on connections of consumers using flows.
     */
    private static final PriceFunctionTO EXTERNAL = PriceFunctionTO.newBuilder()
                    .setExternalPriceFunction(
                                    PriceFunctionTO.ExternalPriceFunction.getDefaultInstance())
                    .build();

    /**
     * Return constant price function with price at 0.
     * Used for flow commodities where key is not FLOW-0
     */
    private static final PriceFunctionTO CONSTANT_ZERO = PriceFunctionTO.newBuilder()
                    .setConstant(Constant.newBuilder().setValue(0).build()).build();

    /**
     * Biclique commodity sold settings.
     */
    public static final CommoditySoldSettingsTO BC_SETTING_TO = CommoditySoldSettingsTO.newBuilder()
                    .setResizable(false)
                    .setPriceFunction(PriceFunctionTO.newBuilder()
                                    .setConstant(Constant.newBuilder().setValue(0).build()).build())
                    .setUpdateFunction(PROJECTED_SECOND_UPDATING_FUNCTION).build();

    /**
     * Select the right {@link PriceFunctionTO} based on the commodity sold type.
     *
     * @param commType {@link CommodityType} object that represents type and key of commodity
     * @param scale    float that represents how much the utilization is scaled to.
     *                 The scale has to be greater than or equal to 1.0 to be meaningful.
     * @param dto the entity whose commodity price function is being set.
     * @return a (reusable) instance of PriceFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    public static PriceFunctionTO priceFunction(CommodityType commType, float scale,
                                                TopologyEntityDTO dto) {
        int commodityType = commType.getType();
        String commodityKey = commType.getKey();
        if (CONSTANT_PRICE_TYPES.contains(commodityType)) {
            return CONSTANT;
        } else if (STEP_PRICE_TYPES.contains(commodityType)) {
            if (dto != null && dto.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE
                && (commodityType == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE
                    || commodityType == CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE)) {
                return IG;
            }
            return STEP;
        } else if (FINITE_SWP_PRICE_TYPES.contains(commodityType)) {
            return FSWP;
        } else if (IMAGE_COMMODITY_SET.contains(commodityType)) {
            return SQRP;
        } else if (FLOW_COMMODITY_SET.contains(commodityType)) {
            if (commodityKey.equals(FLOW_ZERO_KEY)) {
                return EXTERNAL;
            } else {
                return CONSTANT_ZERO;
            }
        } else if (commodityType == CommodityDTO.CommodityType.CPU_VALUE) {
            scale = scale > 1.0f ? scale : 1.0f;
            return PriceFunctionTO.newBuilder()
                    .setScaledCapacityStandardWeighted(PriceFunctionTO
                            .ScaledCapacityStandardWeighted
                            .newBuilder().setScale(scale)
                            .setWeight(1.0f)
                            .build())
                    .build();
        } else {
            return SWP;
        }
    }

    /**
     * Select the right {@link UpdatingFunctionTO} based on the commodity sold type.
     *
     * @param commSoldType the numerical commodity sold type
     * @return a (reusable) instance of UpdatingFunctionTO to use in the commodity sold settings.
     */
    @Nonnull
    public static UpdatingFunctionTO updateFunction(CommodityType commSoldType) {
        int commType = commSoldType.getType();
        if (FINITE_SWP_PRICE_TYPES.contains(commType)
                        || commType == CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE) {
            return AVERAGE_UPDATING_FUNCTION;
        } else if (FLOW_COMMODITY_SET.contains(commType)) {
            return EXTERNAL_UPDATING_FUNCTION;
        } else if (IGNORE_UPDATE_TYPES.contains(commType)) {
            return IGNORE_CONSUMPTION_UPDATING_FUNCTION;
        } else {
            return UpdatingFunctionTO.getDefaultInstance();
        }
    }
}
