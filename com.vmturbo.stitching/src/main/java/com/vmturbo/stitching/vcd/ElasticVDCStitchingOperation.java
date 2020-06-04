package com.vmturbo.stitching.vcd;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualDatacenterRole;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.IntersectionStitchingIndex;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingIndex;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.stitching.utilities.CopyCommodities;

/**
 * Class for Elastic VDC stitching.
 * Internal entity : elastic VDCs from VCD target
 *       Signature : list of replacesEntityId
 * External entity : VDCs from VC target
 *       Signature : UUID
 * Stitching graph example, we can regard the consumer and producer VDC in general
 *    Elastic VDC1 -&gt; [VDC1, VDC2]
 *    Elastic VDC2 -&gt; [VDC3]
 *
 * Before stitching:
 *      VM1  VM2  VM3 VM4
 *       \    /   /    |
 *       VDC1   VDC2  VDC3  &lt;-- [EVDC1, EVDC2]
 *         \   / |     |
 *         PM1  St1   PM2
 *          |  /       |
 *         DC1        DC2
 *
 * After stitching:
 *      VM1  VM2  VM3  VM4
 *        \  /  /       |
 *         EVDC1      EVDC2
 *         \ / \        |
 *         PM1 St1     PM2
 *          |  /        |
 *         DC1         DC2
 */
public class ElasticVDCStitchingOperation implements StitchingOperation<String, String> {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>> getScope(
            @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        // now we only stitch VCD with VC target
        return Optional.of(stitchingScopeFactory.probeScope(SDKProbeType.VCENTER.getProbeType()));
    }

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return EntityType.VIRTUAL_DATACENTER;
    }

    @Nonnull
    @Override
    public Optional<EntityType> getExternalEntityType() {
        return Optional.of(EntityType.VIRTUAL_DATACENTER);
    }

    @Override
    public Collection<String> getInternalSignature(
            @Nonnull final StitchingEntity internalEntity) {
        return internalEntity.getEntityBuilder().getReplacesEntityIdList();
    }

    @Override
    public Collection<String> getExternalSignature(@Nonnull final StitchingEntity externalEntity) {
        return Collections.singleton(externalEntity.getEntityBuilder().getId());
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> stitch(
            @Nonnull final Collection<StitchingPoint> stitchingPoints,
            @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        stitchingPoints.forEach(stitchingPoint -> {
            stitchVMs(stitchingPoint, resultBuilder);
            stitchCommoditiesBought(stitchingPoint, resultBuilder);
        });
        return resultBuilder.build();
    }

    /**
     * Iterate over all of the consumer VMs in VDC from VC, and change the provider and key to the
     * elastic VDC from VCD, if the providers and key is the current VDC UUID.
     *
     * @param stitchingPoint The point at which the elastic VDCs from VCD probe should be stitched
     *                       with the VDCs discovered by VC probe.
     * @param resultBuilder The builder of the results of the stitching operation. Changes to
     *                       relationships made by the stitching operation should be noted
     *                       in these results.
     */
    private void stitchVMs(@Nonnull final StitchingPoint stitchingPoint,
                              @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        stitchingPoint.getExternalMatches().stream()
            .filter(this::isConsumerVDC)
            .forEach(externalEntity -> {
                logger.debug("Stitching VMs from VDC {} to elastic VDC {}.",
                    externalEntity.getDisplayName(),
                    stitchingPoint.getInternalEntity().getDisplayName());
                // iterate all consumers to find the VMs
                externalEntity.getConsumers().stream()
                    .filter(consumer -> consumer.getEntityType() == EntityType.VIRTUAL_MACHINE)
                    .forEach(vm ->
                        resultBuilder.queueChangeRelationships(vm,
                            toUpdate -> updateVMCommoditiesBought(toUpdate,
                                stitchingPoint.getInternalEntity(), externalEntity)));
                // remove the consumer VDCs on VC
                resultBuilder.queueEntityRemoval(externalEntity);
        });
    }

    /**
     * Update the VM commodities bought from VDC on VC to elastic VDC on VCD.
     *
     * @param vm The target VM need to be updated.
     * @param internalEntity The new provider for the VM, here is elastic consumer VDC entity
     * @param externalEntity The old provider for the VM, here is consumer VDC entity
     */
    private void updateVMCommoditiesBought(@Nonnull final StitchingEntity vm,
                                           @Nonnull final StitchingEntity internalEntity,
                                           @Nonnull final StitchingEntity externalEntity) {
        // fetch all the commodities bought from the external VDC
        Optional<List<CommoditiesBought>> commBoughtFromVDCOpt = vm.removeProvider(externalEntity);
        if (!commBoughtFromVDCOpt.isPresent()) {
            return;
        }

        // update commodity key from external entity to internal entity
        commBoughtFromVDCOpt.get().stream()
            .flatMap(commBought -> commBought.getBoughtList().stream())
            .forEach(commBought -> {
                if (commBought.hasKey()) {
                    String internalUUID = internalEntity.getEntityBuilder().getId();
                    String externalUUID = externalEntity.getEntityBuilder().getId();
                    // make sure provider key is external VDC uuid
                    if (commBought.getKey().contains(externalUUID)) {
                        // replace external UUID to internal UUID
                        commBought.setKey(commBought.getKey()
                            .replace(externalUUID, internalUUID));
                    }
                }
            });
        // replace the commodity bought to elastic VDC
        vm.getCommodityBoughtListByProvider().put(internalEntity, commBoughtFromVDCOpt.get());
    }

    /**
     * The Producer VDC does not have any commodity bought since we cannot get host or storage
     * information rom VCD, so we need to copy all the commodities bought in VDC from VC probe to
     * the elastic VDC from VCD probe.
     *
     * @param stitchingPoint The point at which the elastic VDCs from VCD probe should be stitched
     *                       with the VDCs discovered by VC probe.
     * @param resultBuilder The builder of the results of the stitching operation. Changes to
     *                       relationships made by the stitching operation should be noted
     *                       in these results.
     */
    private void stitchCommoditiesBought(@Nonnull final StitchingPoint stitchingPoint,
                                            @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        stitchingPoint.getExternalMatches().stream()
            .filter(this::isProducerVDC)
            .forEach(externalEntity -> {
                logger.debug("Stitching commodities bought from producer VDC {} to elastic " +
                    "producer VDC {}.", externalEntity.getDisplayName(),
                        stitchingPoint.getInternalEntity().getDisplayName());
                // iterate the commodities bought in external VDC and copy them to elastic VDC
                resultBuilder.queueChangeRelationships(stitchingPoint.getInternalEntity(),
                    toUpdate -> CopyCommodities.copyCommodities()
                        .from(externalEntity)
                        .to(toUpdate));
                // remove the producer VDCs on VC
                resultBuilder.queueEntityRemoval(externalEntity);
        });
    }

    /**
     * Check if the stitching entity is consumer VDC.
     *
     * @param stitchingEntity The stitching entity will need to check.
     * @return true if if the entity is consumer VDC
     */
    private boolean isConsumerVDC(@Nonnull final StitchingEntity stitchingEntity) {
        if (!stitchingEntity.getEntityBuilder().hasVirtualDatacenterData()
            || !stitchingEntity.getEntityBuilder().getVirtualDatacenterData().hasVdcTypeProps()) {
            return false;
        }
        return VirtualDatacenterRole.CONSUMER.equals(stitchingEntity.getEntityBuilder()
            .getVirtualDatacenterData().getVdcTypeProps().getRole());
    }

    /**
     * Check if the stitching entity is producer VDC.
     *
     * @param stitchingEntity The stitching entity will need to check.
     * @return true if if the entity is producer VDC
     */
    private boolean isProducerVDC(@Nonnull final StitchingEntity stitchingEntity) {
        if (!stitchingEntity.getEntityBuilder().hasVirtualDatacenterData()
            || !stitchingEntity.getEntityBuilder().getVirtualDatacenterData().hasVdcTypeProps()) {
            return false;
        }
        return VirtualDatacenterRole.PRODUCER.equals(stitchingEntity.getEntityBuilder()
            .getVirtualDatacenterData().getVdcTypeProps().getRole());
    }

    /**
     *  We are using replacesEntityId in Elastic VDC as the key, and the List of replacesEntityId as
     *  value. The replacesEntityId is also the UUID of external VDC entity in VC. According to the
     *  logic we can simply use {@link IntersectionStitchingIndex} to build stitching index.
     *
     *  external signature        internal signature
     *  replacesEntityId -&gt; [replacesEntityId, replacesEntityId2]
     *  replacesEntityId2 -&gt; [replacesEntityId, replacesEntityId2]
     *
     *  We find all the external VDC which UUID match the key in the stitching index, and create
     *  matching pair for the matched internal Elastic VDC and external VDC.
     */
    @Nonnull
    @Override
    public StitchingIndex<String, String> createIndex(final int expectedSize) {
        return new IntersectionStitchingIndex(expectedSize);
    }

}
