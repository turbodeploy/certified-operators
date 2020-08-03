package com.vmturbo.stitching.prestitching;

import static com.vmturbo.stitching.utilities.MergeEntities.mergeEntity;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PowerState;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.AccessAndLatency;
import com.vmturbo.stitching.utilities.EntityFieldMergers;
import com.vmturbo.stitching.utilities.EntityScopeFilters;

/**
 * Shared storages are storage entities that may be shared across multiple targets. Each target will discover
 * the target and provide its own results in the form of an {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO}
 * describing the instance.
 *
 * These multiple instances will share the same OID and should be merged down into a single combined instance of
 * the entity. In general, the merging attempts to preserve the values from the most recently discovered instance
 * of this entity over other values. In the cases where a value must be computed from the combined information from
 * all the instances at once, the combined value is computed and applied to the instance of the entity that is
 * retained from the merger.
 */
public class SharedStoragePreStitchingOperation implements PreStitchingOperation {
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
        @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        // Apply this calculation to all storages discovered by hypervisor probes.
        return stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.HYPERVISOR, EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(@Nonnull Stream<StitchingEntity> entities,
                                                                  @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        EntityScopeFilters.sharedEntitiesByOid(entities).forEach(sharedStorages ->
            mergeSharedStorages(sharedStorages, resultBuilder));

        return resultBuilder.build();
    }

    /**
     * Merge multiple instances of the shared storage down to a single instance.
     *
     * Certain commodities such as storageProvisioned, storageLatency, and storageIOPS require special calculations,
     * but for most commodities we use the heuristic that we simply retain the most recently discovered measurements.
     *
     * @param sharedStorageInstances The shared storages. Note that there must be at least 2 storages in the group
     *                               of shared storages.
     * @param resultBuilder The builder for the result of the stitching calculation.
     */
    private void mergeSharedStorages(@Nonnull final List<StitchingEntity> sharedStorageInstances,
                                     @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        Preconditions.checkArgument(sharedStorageInstances.size() > 1, "There must be multiple instances of a " +
            "shared storage.");
        logger.debug("Merging shared storages: {}", sharedStorageInstances);

        // Find the most recently updated shared storage.
        final StitchingEntity mostUpToDate = sharedStorageInstances.stream()
            .sorted((a, b) -> Long.compare(b.getLastUpdatedTime(), a.getLastUpdatedTime())) // Sort descending
            .findFirst()
            .get();

        // Remove the duplicate instances of this object (and their associated providers) because we now
        // have all the data we require from them.
        sharedStorageInstances.stream()
            .filter(storageInstance -> storageInstance != mostUpToDate)
            .forEach(duplicateInstance ->
                mergeStorageAndStorageProviders(duplicateInstance, mostUpToDate, resultBuilder));

        // Retain the most up to date so that we have the most up to date metrics. Merge shared data onto
        // this authoritative instance.
        OptionalDouble storageProvisionedUsed = calculateStorageProvisionedUsed(mostUpToDate, sharedStorageInstances);
        OptionalDouble storageLatencyUsed = calculateStorageLatencyUsed(sharedStorageInstances);
        OptionalDouble storageAccessUsed = calculateStorageAccessUsed(sharedStorageInstances);
        PowerState mergedPowerState = calculatePowerState(sharedStorageInstances);

        resultBuilder.queueUpdateEntityAlone(mostUpToDate, entity -> {
                updateCommoditySoldUsed(entity, CommodityType.STORAGE_PROVISIONED, storageProvisionedUsed);
                updateCommoditySoldUsed(entity, CommodityType.STORAGE_LATENCY, storageLatencyUsed);
                updateCommoditySoldUsed(entity, CommodityType.STORAGE_ACCESS, storageAccessUsed);
                entity.getEntityBuilder().setPowerState(mergedPowerState);
            });
    }

    private void mergeStorageAndStorageProviders(@Nonnull final StitchingEntity duplicateInstance,
                                                 @Nonnull final StitchingEntity mostUpToDateInstance,
                                                 @Nonnull final StitchingChangesBuilder resultBuilder) {
        resultBuilder.queueEntityMerger(mergeEntity(duplicateInstance)
            .onto(mostUpToDateInstance)
            // Keep the displayName for the entity alphabetically first to prevent ping-ponging
            .addFieldMerger(EntityFieldMergers.DISPLAY_NAME_LEXICOGRAPHICALLY_FIRST));

        // Remove the providers that are shared between the two instances.
        duplicateInstance.getProviders().stream()
            .map(StitchingEntity::getEntityType)
            .forEach(providerType -> mergeProviders(providerType,
                duplicateInstance.getProviders().stream(),
                mostUpToDateInstance.getProviders().stream(),
                resultBuilder));
    }

    /**
     * Merge the providers on a storage down to a single instance.
     * If a provider of the given type is not present on both the duplicate and upToDate instance,
     * a warning is logged but no merge is performed.
     *
     * @param typeToMerge The type of provider to merge.
     * @param duplicateInstanceProviders The providers to the duplicate instance.
     * @param mostUpToDateInstanceProviders The providers to the up-to-date instance.
     * @param resultBuilder The {@link StitchingChangesBuilder} on which to add
     *                      the merge details.
     */
    private void mergeProviders(@Nonnull final EntityType typeToMerge,
                                @Nonnull final Stream<StitchingEntity> duplicateInstanceProviders,
                                @Nonnull final Stream<StitchingEntity> mostUpToDateInstanceProviders,
                                @Nonnull final StitchingChangesBuilder resultBuilder) {
        // Remove the disk array supply the duplicate instance. We'll retain the disk array supplying
        // the mostUpToDate instance.
        final Optional<StitchingEntity> duplicateProvider = duplicateInstanceProviders
            .filter(provider -> provider.getEntityType() == typeToMerge)
            .findFirst();
        final Optional<StitchingEntity> upToDateProvider = mostUpToDateInstanceProviders
            .filter(provider -> provider.getEntityType() == typeToMerge)
            .findFirst();
        if (duplicateProvider.isPresent() && upToDateProvider.isPresent()) {
            final StitchingEntity duplicateDa = duplicateProvider.get();
            final StitchingEntity uptoDateDa = upToDateProvider.get();
            Preconditions.checkArgument(duplicateDa.getOid() == uptoDateDa.getOid(),
                "Providers {} and {} should have equal OIDs.", duplicateDa, uptoDateDa);

            // Merge the disk arrays as well so that we retain the fact that the disk array
            // was discovered by multiple targets.
            resultBuilder.queueEntityMerger(mergeEntity(duplicateDa)
                .onto(uptoDateDa)
                    // Keep the displayName for the entity alphabetically first to prevent ping-ponging
                .addFieldMerger(EntityFieldMergers.DISPLAY_NAME_LEXICOGRAPHICALLY_FIRST));
        } else {
            logger.warn("Missing provider on shared storage. duplicatePresent={}, upToDatePresent={}.",
                duplicateProvider.isPresent(), upToDateProvider.isPresent());
        }

    }

    private void updateCommoditySoldUsed(@Nonnull final StitchingEntity mostUpToDate,
                                         @Nonnull final CommodityType commodityType,
                                         final OptionalDouble value) {
        Objects.requireNonNull(commodityType);
        if (!value.isPresent()) {
            return; // The value is not present. Do not set it.
        }

        Optional<CommodityDTO.Builder> optionalCommoditySold = mostUpToDate.getCommoditiesSold()
            .filter(commodityDTO -> commodityDTO.getCommodityType() == commodityType)
            .findFirst();
        if (!optionalCommoditySold.isPresent()) {
            logger.warn("{} does not sell the {} commodity. Skipping calculation update.",
                mostUpToDate, commodityType);
            return;
        }

        optionalCommoditySold.get().setUsed(value.getAsDouble());
    }

    private OptionalDouble calculateStorageProvisionedUsed(@Nonnull final StitchingEntity mostUpToDate,
                                                   @Nonnull final List<StitchingEntity> storageInstances) {
        final double overProvisioned = storageInstances.stream()
            .mapToDouble(this::calculateOverProvisioned)
            .sum();

        // The total amount provisioned is the sum of the overprovisioned from each discovered instance
        // plus the most up-to-date value of the storage amount.
        double latestStorageAmount = mostUpToDate.getCommoditiesSold()
            .filter(commoditySold -> commoditySold.getCommodityType() == CommodityType.STORAGE_AMOUNT)
            .findFirst()
            .map(CommodityDTO.Builder::getUsed)
            .orElse(0.0);

        return OptionalDouble.of(latestStorageAmount + overProvisioned);
    }

    /**
     * Calculate storage latency used as the maximum of all the discovered latencies.
     *
     * @param storageInstances The shared storage instances whose latency should be calcualated.
     * @return The calculated latency, or if no latency values are present, {@link OptionalDouble#empty()}
     */
    private OptionalDouble calculateStorageLatencyUsed(@Nonnull final List<StitchingEntity> storageInstances) {
        /**
         * For additional details see {@link AccessAndLatency::latencyWeightedAveraged}.
         */
        final List<AccessAndLatency> accessAndLatencies = storageInstances.stream()
            .map(storage -> AccessAndLatency.accessAndLatencyFromCommodityDtos(
                storage.getCommoditiesSold()
                    .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_ACCESS)
                    .findFirst(),
                storage.getCommoditiesSold()
                    .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_LATENCY)
                    .findFirst()))
            .filter(AccessAndLatency::hasLatency)
            .collect(Collectors.toList());

        if (accessAndLatencies.isEmpty()) {
            return OptionalDouble.empty();
        }

        return OptionalDouble.of(AccessAndLatency.latencyWeightedAveraged(accessAndLatencies.stream()));
    }

    /**
     * Calculate storage access used as the sum of all the IOPS (access).
     *
     * @param storageInstances The shared storage instances whose access should be calcualated.
     * @return The calculated access.
     */
    private OptionalDouble calculateStorageAccessUsed(@Nonnull final List<StitchingEntity> storageInstances) {
        return OptionalDouble.of(storageInstances.stream()
            .flatMap(storage -> storage.getCommoditiesSold()
                .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_ACCESS)
                .filter(Builder::hasUsed))
            .mapToDouble(Builder::getUsed)
            .sum());
    }

    /**
     * Calculate the best power state of the shared storages.
     * eg. if there is at least one POWERED_ON among the storages, the final power state will be POWERED_ON
     * @param storageInstances The shared storage instances.
     * @return The calculated PowerState
     */
    @Nonnull
    private PowerState calculatePowerState(@Nonnull final List<StitchingEntity> storageInstances) {
        return storageInstances.stream().map(StitchingEntity::getEntityBuilder)
                .filter(entity -> entity.getOrigin() == EntityOrigin.DISCOVERED)
                .filter(EntityDTO.Builder::hasPowerState).map(EntityDTO.Builder::getPowerState)
                .reduce(PowerState.POWERSTATE_UNKNOWN, (state1, state2) ->
                        //POWERED_ON < POWERED_OFF < SUSPENDED < POWERSTATE_UNKNOWN
                        state1.getNumber() < state2.getNumber() ? state1 : state2);
    }

    /**
     * Per Dmitry in explanation of this logic:
     * We, like VMware, understand provisioned as 'theoretically used' - requested but not committed.
     * In VC you can have individual vm disks created as thin provisioned. You request some capacity
     * but the actual usage on disk is much lower. So 'provisioned' that we report is the sum of
     * capacities of vm disks, thin provisioned ones make it much higher. Plus we add temporary VC
     * storage reservation for the ongoing DRS.
     *
     * Now if you look at the shared storage in this model, apparently one VC does not know of thin
     * provisioning or temporary overhead of another VC. But the actual physical usage (StorageAmount.used)
     * at one point in time is the same, no matter from what VC you are looking at the storage.
     *
     * In somewhat less technical terms, this method attempts to calculate how much a particular VC instance
     * sees a storage as being overprovisioned (that is, the amount of storage provisioned but not committed)
     * which is the difference between its provisioned.used and amount.used values.
     *
     * @param stitchingEntity The entity whose overprovisioned value is to be calculated.
     * @return The amount the entity is overprovisioned.
     */
    private double calculateOverProvisioned(@Nonnull final StitchingEntity stitchingEntity) {
        Optional<CommodityDTO.Builder> storageProvisioned = stitchingEntity.getCommoditiesSold()
            .filter(sold -> sold.getCommodityType() == CommodityType.STORAGE_PROVISIONED)
            .findFirst();

        Optional<CommodityDTO.Builder> storageAmount = stitchingEntity.getCommoditiesSold()
            .filter(sold -> sold.getCommodityType() == CommodityType.STORAGE_AMOUNT)
            .findFirst();

        if (storageProvisioned.isPresent() && storageProvisioned.get().hasUsed()) {
            double provisionedUsed = storageProvisioned.get().getUsed();

            if (storageAmount.isPresent() && storageAmount.get().hasUsed()) {
                double amountUsed = storageAmount.get().getUsed();
                //storageProvionsedUsed is computed using consumers' used, so it can be < or > amount used.
                if (provisionedUsed >= amountUsed) {
                    return (provisionedUsed - amountUsed);
                }
            } else {
                logger.error("{}: Storage provisioned is specified but storage amount is not.", stitchingEntity);
            }
        }

        return 0;
    }
}
