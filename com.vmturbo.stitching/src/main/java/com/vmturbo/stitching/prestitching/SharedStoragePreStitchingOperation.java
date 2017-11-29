package com.vmturbo.stitching.prestitching;

import static com.vmturbo.stitching.utilities.MergeEntities.mergeEntity;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingResult;
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
    public CalculationScope getCalculationScope(@Nonnull CalculationScopeFactory calculationScopeFactory) {
        // Apply this calculation to all storages discovered by hypervisor probes.
        return calculationScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.HYPERVISOR, EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public StitchingResult performOperation(@Nonnull Stream<StitchingEntity> entities,
                                            @Nonnull StitchingResult.Builder resultBuilder) {
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
                                     @Nonnull final StitchingResult.Builder resultBuilder) {
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

        resultBuilder.queueUpdateEntityAlone(mostUpToDate, entity -> {
                updateCommoditySoldUsed(entity, CommodityType.STORAGE_PROVISIONED, storageProvisionedUsed);
                updateCommoditySoldUsed(entity, CommodityType.STORAGE_LATENCY, storageLatencyUsed);
                updateCommoditySoldUsed(entity, CommodityType.STORAGE_ACCESS, storageAccessUsed);
            });
    }

    private void mergeStorageAndStorageProviders(@Nonnull final StitchingEntity duplicateInstance,
                                                 @Nonnull final StitchingEntity mostUpToDateInstance,
                                                 @Nonnull final StitchingResult.Builder resultBuilder) {
        resultBuilder.queueEntityMerger(mergeEntity(duplicateInstance).onto(mostUpToDateInstance));

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
     * @param resultBuilder The {@link com.vmturbo.stitching.StitchingResult.Builder} on which to add
     *                      the merge details.
     */
    private void mergeProviders(@Nonnull final EntityType typeToMerge,
                                @Nonnull final Stream<StitchingEntity> duplicateInstanceProviders,
                                @Nonnull final Stream<StitchingEntity> mostUpToDateInstanceProviders,
                                @Nonnull final StitchingResult.Builder resultBuilder) {
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
            resultBuilder.queueEntityMerger(mergeEntity(duplicateDa).onto(uptoDateDa));
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
         * Per Rich Hammond:
         * The storage probe group has moved to use an IOPS weighted average of the latency values.
         * We found that using max [or even uniform average] resulted in market actions which made no sense to
         * the customer. The intent of the IOPS weighted average is to solve the request size versus latency issue.
         * We cannot tell the size of individual requests, but larger requests, which have higher latency,
         * should not occur as frequently, so weighting by IOPS count will give greater weight to the small,
         * fast, queries without ignoring the larger, slower queries.
         */
        final List<LatencyAndIops> latencyAndIopses = storageInstances.stream()
            .map(storage -> new LatencyAndIops(
                storage.getCommoditiesSold()
                    .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_LATENCY)
                    .findFirst(),
                storage.getCommoditiesSold()
                    .filter(commodity -> commodity.getCommodityType() == CommodityType.STORAGE_ACCESS)
                    .findFirst()))
            .filter(LatencyAndIops::hasLatency)
            .collect(Collectors.toList());

        if (latencyAndIopses.isEmpty()) {
            return OptionalDouble.empty();
        }

        final double weightedLatencySum = latencyAndIopses.stream()
            .map(LatencyAndIops::weightedLatency)
            .filter(Optional::isPresent)
            .mapToDouble(Optional::get)
            .sum();
        final double iopsSum = latencyAndIopses.stream()
            .mapToDouble(LatencyAndIops::iopsWeight)
            .sum();

        return OptionalDouble.of(weightedLatencySum / iopsSum);
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
                if (provisionedUsed >= amountUsed) {
                    return (provisionedUsed - amountUsed);
                } else {
                    logger.error("{}: Storage provisioned used {} is smaller than storage amount used {}",
                        stitchingEntity, provisionedUsed, amountUsed);
                }
            } else {
                logger.error("{}: Storage provisioned is specified but storage amount is not.", stitchingEntity);
            }
        }

        return 0;
    }

    /**
     * A small helper class that bundles latency and IOPS commodities together.
     */
    private static class LatencyAndIops {
        public final Optional<Double> latencyUsed;

        public final Optional<Double> iopsUsed;

        public LatencyAndIops(@Nonnull final Optional<CommodityDTO.Builder> latency,
                              @Nonnull final Optional<CommodityDTO.Builder> iops) {
            latencyUsed = latency
                .filter(CommodityDTO.Builder::hasUsed)
                .map(CommodityDTO.Builder::getUsed);
            iopsUsed = iops
                .filter(CommodityDTO.Builder::hasUsed)
                .map(CommodityDTO.Builder::getUsed)
                // If the probe cannot measure a real value, it often provides a value of 0 or <0
                // and we will calculate a uniform average.
                .map(value -> value <= 0.0 ? 1.0 : value);
        }

        /**
         * Whether the latency is present in the {@link LatencyAndIops}.
         *
         * @return Whether the latency is present in the {@link LatencyAndIops}.
         */
        public boolean hasLatency() {
            return latencyUsed.isPresent();
        }

        /**
         * Return the IOPS weight. If no IOPS is present, returns a 1.0 to act as a uniform weight.
         *
         * @return The IOPS weight, or 1.0 if the IOPS value is not present.
         */
        public double iopsWeight() {
            return iopsUsed.orElse(1.0);
        }

        /**
         * The latency value weighted by the IOPS value, that is latency*IOPS.
         * If latency is not present, returns empty.
         *
         * @return The IOPS weight, or 1.0 if the IOPS value is not present.
         */
        public Optional<Double> weightedLatency() {
            return latencyUsed.map(latency -> latency * iopsWeight());
        }
    }
}
