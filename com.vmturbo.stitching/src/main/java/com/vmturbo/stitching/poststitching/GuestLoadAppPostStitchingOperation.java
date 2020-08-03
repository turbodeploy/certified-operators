package com.vmturbo.stitching.poststitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * A post-stitching operation that adjusts the used value of the GuestLoad commodities.
 *
 * <p>The calculation is based on other stitched apps or containers related to the hosting VM, and
 * is defined as: guestload used = vm sold - total used by other consumers.
 *
 * <p>Suppose vm1 which sells VCPU (used: 200). guestloadApp1 buys VCPU (used: 150) from vm1, and
 * realApp1 buys VCPU (used: 160), then after this calculation, VCPU used value on guestloadApp1
 * should be 40 (= 200 - 160).
 */
public class GuestLoadAppPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    static final String APPLICATION_TYPE_PATH = "common_dto.EntityDTO.ApplicationData.type";

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.APPLICATION_COMPONENT);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        // go through all the GuestLoad applications and adjust used value
        entities
            .filter(GuestLoadAppPostStitchingOperation::isGuestLoadApplication)
            .forEach(guestLoad -> resultBuilder.queueUpdateEntityAlone(guestLoad,
                guestLoadApp -> guestLoadApp.getProviders().forEach(
                        vm -> adjustGuestLoadCommodityUsed(guestLoadApp, vm))));
        return resultBuilder.build();
    }

    /**
     * Adjust the used value of relevant commodities for the given GuestLoad application.
     *
     * @param guestLoad the TopologyEntity for the GuestLoad app
     * @param vm the provider for this GuestLoad app
     */
    private void adjustGuestLoadCommodityUsed(@Nonnull TopologyEntity guestLoad,
                                              @Nonnull TopologyEntity vm) {
        // collect used value of VM sold commodities
        final Table<Integer, String, Double> vmSoldCommoditiesUsed =
            getVMSoldUsedByCommodityTypeAndKey(vm);
        // calculate total used by other consumers (excluding the original used by the GuestLoad)
        final Table<Integer, String, Double> vmOtherConsumersUsedExcludingGuestLoad =
            getVMConsumersTotalUsedExcludingGuestLoad(vm, guestLoad.getOid());

        // go through each of the bought commodities on GuestLoad and adjust used
        guestLoad.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersBuilderList().stream()
            .filter(commodityBought -> commodityBought.getProviderId() == vm.getOid())
            .flatMap(commodityBought -> commodityBought.getCommodityBoughtBuilderList().stream())
            .filter(commodityBoughtDTO -> shouldAdjustUsedForCommodity(
                commodityBoughtDTO.getCommodityType().getType()))
            .forEach(commodityBoughtDTO -> {
                final Integer type = commodityBoughtDTO.getCommodityType().getType();
                final String key = commodityBoughtDTO.getCommodityType().getKey();
                final Double vmSoldUsed = vmSoldCommoditiesUsed.get(type, key);
                if (vmSoldUsed == null) {
                    // this should not happen, just a safe check and logging
                    logger.error("VM {} is not selling commodity of type {} and key {}, thus " +
                        "commodity used value of GuestLoad application {} is not adjusted",
                        vm, type, key, guestLoad);
                    return;
                }

                final Double totalOtherUsed = vmOtherConsumersUsedExcludingGuestLoad.get(type, key);
                if (totalOtherUsed == null) {
                    // this may happen, since vm may not host other applications, or they don't
                    // sell those commodities we are interested in
                    logger.trace("No other consumers of VM {} is selling commodity " +
                            "of type {} and key {}, thus commodity used value of GuestLoad " +
                            "application {} is not adjusted", vm, type, key, guestLoad);
                    return;
                }

                if (vmSoldUsed < totalOtherUsed) {
                    // Logging at the trace level only, since this may happen a lot especially when
                    // the infrastructure is vSphere, who measures active memory, which may be very
                    // different than the app or container reports (which is from the guest).
                    // Here is a good article about this:
                    // http://itknowledgeexchange.techtarget.com/sql-server/why-dont-the-memory-usage-numbers-from-task-manager-and-vsphere-match
                    logger.trace("Setting 0.0 for the commodity [type {}, key {}] of GuestLoad " +
                            "application {}, because the VM {} sold used ({}) is " +
                            "less than total used ({}) by all other consumers",
                        type, key, guestLoad, vm, vmSoldUsed, totalOtherUsed);
                    commodityBoughtDTO.setUsed(0.0);
                } else {
                    // set the used of GuestLoad app to be the difference between VM's sold used
                    // and the total used of other consumers on the VM
                    final double newUsed = vmSoldUsed - totalOtherUsed;
                    logger.trace("Setting used of commodity [type {}, key {}] of GuestLoad " +
                            "application {} from {} to {}, vm sold: {}, total other: {}",
                        type, key, guestLoad, commodityBoughtDTO.getUsed(), newUsed, vmSoldUsed,
                        totalOtherUsed);
                    commodityBoughtDTO.setUsed(newUsed);
                }
            });
    }

    /**
     * Collect VM sold commodities' used value by commodity type and key.
     *
     * @param vm TopologyEntity for the virtual machine
     * @return table of commodity used value by commodity type and commodity key
     */
    private static Table<Integer, String, Double> getVMSoldUsedByCommodityTypeAndKey(
            @Nonnull final TopologyEntity vm) {
        return vm.getTopologyEntityDtoBuilder()
            .getCommoditySoldListList().stream()
            .filter(commoditySoldDTO -> shouldAdjustUsedForCommodity(
                    commoditySoldDTO.getCommodityType().getType()))
            .collect(ImmutableTable.toImmutableTable(
                    commoditySoldDTO -> commoditySoldDTO.getCommodityType().getType(),
                    commoditySoldDTO -> commoditySoldDTO.getCommodityType().getKey(),
                    CommoditySoldDTO::getUsed,
                    (used1, used2) -> used1 + used2
            ));
    }

    /**
     * Sum the total commodity used value of the other consumers (excluding GuestLoad app) on the
     * VM, and put in a table by commodity type and key.
     *
     * @param vm the vm to get consumers' commodities' used
     * @param guestLoadOid oid of the GuestLoad app to exclude
     * @return table of commodity used value by commodity type and commodity key
     */
    private static Table<Integer, String, Double> getVMConsumersTotalUsedExcludingGuestLoad(
            @Nonnull final TopologyEntity vm, final long guestLoadOid) {
        return vm.getConsumers().stream()
            // excluding the original used by the GuestLoad
            .filter(consumer -> consumer.getOid() != guestLoadOid)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .flatMap(app -> app.getCommoditiesBoughtFromProvidersList().stream())
            .filter(commoditiesBought -> commoditiesBought.getProviderId() == vm.getOid())
            .flatMap(commoditiesBought -> commoditiesBought.getCommodityBoughtList().stream())
            .filter(commodityBoughtDTO -> shouldAdjustUsedForCommodity(
                commodityBoughtDTO.getCommodityType().getType()))
            .collect(ImmutableTable.toImmutableTable(
                commodityBoughtDTO -> commodityBoughtDTO.getCommodityType().getType(),
                commodityBoughtDTO -> commodityBoughtDTO.getCommodityType().getKey(),
                CommodityBoughtDTO::getUsed,
                // sum all used
                (used1, used2) -> used1 + used2
            ));
    }

    /**
     * Check whether the used value of the given commodity type (on the GuestLoad application)
     * should be adjusted.
     *
     * @param commodityType type of the commodity to check
     * @return true if used value of the given commodity type should be adjusted, otherwise false
     */
    private static boolean shouldAdjustUsedForCommodity(int commodityType) {
        return !AnalysisUtil.ACCESS_COMMODITY_TYPES.contains(commodityType);
    }

    /**
     * Check if the given entity is a GuestLoad application.
     *
     * @param entity the entity to check
     * @return true if the entity is a GuestLoad application, otherwise false
     */
    static boolean isGuestLoadApplication(@Nonnull TopologyEntity entity) {
        TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
        return (EntityType.APPLICATION_VALUE == entityBuilder.getEntityType()
                || EntityType.APPLICATION_COMPONENT_VALUE == entityBuilder.getEntityType())
                && SupplyChainConstants.GUEST_LOAD.equals(
                entityBuilder.getEntityPropertyMapMap().get(APPLICATION_TYPE_PATH));
    }
}
