package com.vmturbo.stitching.fabric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.MergeEntities;

/**
 * Stitch a physical machine discovered by a fabric probe with the corresponding physical machine
 * from a hypervisor probe.  Match is made using the PM_UUID property of the physical machine
 * discovered by the fabric probe.  This is a comma separated list of UUIDs that is parsed and
 * matched against UUIDs from physical machines discovered by hypervisor probes. Once matched, we
 * check if the fabric physical machine has a datacenter provider.  If so, we remove it and its
 * commodities as this is a replaceable datacenter and we will use the datacenter from the
 * hypervisor probe as the provider. In case `keepDCsAfterFabricStitching` flag is
 * <ul>
 *     <li>
 *         false, then we
 *   copy over all other commodities from the fabric physical machine to the hypervisor physical
 *   machine. We delete the fabric physical machine as it is a proxy. We remove DC as a provider
 *   for hypervisor physical machine.
 *   </li>
 *   <li>
 *       true, then we  copy over all other commodities from the fabric physical machine to the
 *       hypervisor physical machine. We remove non-access commodities bought by PM from a DC that
 *       have the same commodity type as Fabric PM is buying from Chassis.
 *     </li>
 * </ul>
 */
public class FabricPMStitchingOperation extends FabricStitchingOperation {
    private final boolean keepDCsAfterFabricStitching;

    /**
     * Creates {@link FabricPMStitchingOperation} instance that is going to stitch PMs collected by
     * Fabric probe with PMs collected by hypervisor probe.
     *
     * @param keepDCsAfterFabricStitching flag which is shown whether we going to
     *                 keep hypervisor DCs as providers for hypervisor hosts, by default it is
     *                 {@code false} and DCs are going to be replaced with chassis.
     */
    public FabricPMStitchingOperation(boolean keepDCsAfterFabricStitching) {
        this.keepDCsAfterFabricStitching = keepDCsAfterFabricStitching;
    }

    @Override
    protected String getEntityPropertyName() {
        return "PM_UUID";
    }

    @Override
    protected void stitch(@Nonnull final StitchingPoint stitchingPoint,
                          @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // The PM discovered by the fabric probe
        final StitchingEntity fabricPM = stitchingPoint.getInternalEntity();

        // The PM discovered by the hypervisor probe
        final StitchingEntity hypervisorPM = stitchingPoint.getExternalMatches().iterator().next();

        // We need to handle both UCS-B and UCS-C cases.  In UCS-B case the fabric PM will be
        // hosted by a Chassis, but in UCS-C it will be hosted by a fake Datacenter.  In that case
        // we need to remove the fake Datacenter as a provider and use the hypervisor Datacenter
        // instead.
        // TODO we need to figure out if this is a more general use case or only UCS specific.  If
        // it is the latter we should abstract this logic out to a UCS specific class.
        findProvider(fabricPM, EntityType.DATACENTER)
                .ifPresent(fabricDC ->
                        resultBuilder.queueChangeRelationships(fabricPM,
                                        toUpdate -> toUpdate.removeProvider(fabricDC)));
        findProvider(fabricPM, EntityType.CHASSIS).ifPresent(chassis -> {
            findProvider(hypervisorPM, EntityType.DATACENTER).ifPresent(dc -> {
                if (keepDCsAfterFabricStitching) {
                    resultBuilder.queueChangeRelationships(chassis, ch -> {
                        ch.addConnectedTo(ConnectionType.NORMAL_CONNECTION,
                                        Collections.singleton(dc));
                    });
                } else {
                    resultBuilder.queueChangeRelationships(hypervisorPM,
                                    toUpdate -> toUpdate.removeProvider(dc));
                }
            });
        });

        logger.debug("Stitching UCS PM {} with hypervisor PM {}",
                fabricPM.getDisplayName(), hypervisorPM.getDisplayName());

        resultBuilder
                /*
                 Copies all commodities from fabricPM bought from fabric providers(Chassis,
                 IO modules, ...) into hypervisor PM. Hypervisor PM commodities bought from
                 hypervisor DC that have the same type as fabric PM bought from the Chassis will
                 be removed. In most cases Power and Cooling commodities from relationships
                 PM-Chassis will replace the same commodities in relationships PM-DC. Access
                 commodities like `Datacenter` commodity will remain untouched in case flag is
                 enabled otherwise access commodities will be removed too.
                 */
                .queueChangeRelationships(hypervisorPM,
                        toUpdate -> {
                            fabricPM.getCommodityBoughtListByProvider().forEach((provider, commodityBoughts) -> {
                                final List<CommoditiesBought> cbs =
                                                mergeBoughtCommodities(toUpdate, provider,
                                                                commodityBoughts);
                                if (keepDCsAfterFabricStitching) {
                                    clearDcCommoditiesBoughtFromChassis(toUpdate, provider, cbs);
                                }
                                toUpdate.getCommodityBoughtListByProvider().put(provider, cbs);
                            });
                        })
                // Merge the fabric-probe discovered PM (required for USC-D action execution)
                // ignore the sold commodities from fabricPM, and only keep those from hypervisorPM
                .queueEntityMerger(MergeEntities.mergeEntity(fabricPM).onto(hypervisorPM,
                    MergeEntities.DROP_ALL_FROM_COMMODITIES_STRATEGY));
    }

    private static List<CommoditiesBought> mergeBoughtCommodities(StitchingEntity toUpdate,
                    StitchingEntity provider, Collection<CommoditiesBought> commodityBoughts) {
        final List<CommoditiesBought> result = new ArrayList<>(commodityBoughts.size());
        for (CommoditiesBought boughts : commodityBoughts) {
            final Optional<CommoditiesBought> matchingBoughts =
                            toUpdate.getMatchingCommoditiesBought(provider, boughts);
            final List<CommodityDTO.Builder> merged =
                            CopyCommodities.mergeCommoditiesBought(boughts.getBoughtList(),
                                            matchingBoughts.map(CommoditiesBought::getBoughtList)
                                                            .orElse(null), null, false);
            result.add(new CommoditiesBought(merged));
        }
        return result;
    }

    private void clearDcCommoditiesBoughtFromChassis(StitchingEntity toUpdate,
                    StitchingEntity provider, Collection<CommoditiesBought> boughtFromChassis) {
        if (provider.getEntityType() != EntityType.CHASSIS) {
            return;
        }
        final Predicate<CommodityDTO.Builder> commoditiesToRemovePredicate;
        if (keepDCsAfterFabricStitching) {
            commoditiesToRemovePredicate = b -> !AnalysisUtil.ACCESS_COMMODITY_TYPES.contains(
                            b.getCommodityType().getNumber());
        } else {
            commoditiesToRemovePredicate = b -> true;
        }
        final Collection<CommodityType> commodityTypesToRemove =
                        boughtFromChassis.stream().map(CommoditiesBought::getBoughtList)
                                        .flatMap(Collection::stream)
                                        .filter(commoditiesToRemovePredicate)
                                        .map(Builder::getCommodityType).collect(Collectors.toSet());
        for (Entry<StitchingEntity, List<CommoditiesBought>> providerToCommodityBoughts : toUpdate.getCommodityBoughtListByProvider()
                        .entrySet()) {
            if (providerToCommodityBoughts.getKey().getEntityType() != EntityType.DATACENTER) {
                continue;
            }
            for (CommoditiesBought boughts : providerToCommodityBoughts.getValue()) {
                boughts.getBoughtList().removeIf(b -> commodityTypesToRemove.contains(
                                b.getCommodityType()));
            }
        }
    }

    @Nonnull
    private static Optional<StitchingEntity> findProvider(@Nonnull StitchingEntity consumer,
                    @Nonnull EntityType providerType) {
        return consumer.getProviders().stream()
                        .filter(entity -> entity.getEntityType() == providerType).findAny();
    }

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return EntityType.PHYSICAL_MACHINE;
    }
}
