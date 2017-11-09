package com.vmturbo.stitching.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingIndex;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationResult;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.utilities.CopyCommodities;

/**
 * A stitching operation appropriate for use by storage targets.
 *
 * Matching:
 * - Matches internal storages with external storages.
 * - A match is determined when the external names for an internal storage form a non-empty set
 *   intersection with the external names for an external storage.
 * Processing:
 * - Remove hypervisor disk array
 * - Remove storage storage
 * - Connect hypervisor storage to storage disk array using commodities from the storage storage.
 *
 *      Hypervisor    Storage
 *
 *         App
 *          |
 *         VM
 *        /  \   connect
 *       PM  ST--------* ST <--delete
 *       |    |         \ |
 *       DC  DA<-delete  DA
 *                        |
 *                       SC
 */
public class StorageStitchingOperation implements StitchingOperation<List<String>, List<String>> {
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return EntityType.STORAGE;
    }

    @Nonnull
    @Override
    public Optional<EntityType> getExternalEntityType() {
        return Optional.of(EntityType.STORAGE);
    }

    @Override
    public Optional<List<String>> getInternalSignature(@Nonnull StitchingEntity internalEntity) {
        final List<String> externalNames = internalEntity.getEntityBuilder().getStorageData().getExternalNameList();
        return externalNames.isEmpty() ? Optional.empty() : Optional.of(externalNames);
    }

    @Override
    public Optional<List<String>> getExternalSignature(@Nonnull StitchingEntity externalEntity) {
        return getInternalSignature(externalEntity);
    }

    @Nonnull
    @Override
    public StitchingOperationResult stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                           @Nonnull final StitchingOperationResult.Builder resultBuilder) {
        stitchingPoints.forEach(stitchingPoint -> stitch(stitchingPoint, resultBuilder));

        return resultBuilder.build();
    }

    /**
     * Remove the hypervisor-probe disk array
     * Remove the storage-probe storage
     * Connect the hypervisor-probe storage to the storage-probe disk array.
     *
     * @param stitchingPoint The point at which the storage graph should be stitched with
     *                       the graphs discovered by external probes.
     * @param resultBuilder The builder of the results of the stitching operation. Changes to
     *                      relationships made by the stitching operation should be noted
     *                      in these results.
     */
    private void stitch(@Nonnull final StitchingPoint stitchingPoint,
                        @Nonnull final StitchingOperationResult.Builder resultBuilder) {
        // The storage discovered by the storage probe
        final StitchingEntity storageStorage = stitchingPoint.getInternalEntity();

        // The storage discovered by the hypervisor probe
        final StitchingEntity hypervisorStorage = stitchingPoint.getExternalMatches().iterator().next();

        // Find the disk array discovered by the hypervisor probe by finding the provider
        // of the hypervisor storage
        final StitchingEntity hypervisorDiskArray = hypervisorStorage.getProviders().stream()
            .filter(entity -> entity.getEntityType() == EntityType.DISK_ARRAY)
            .findFirst()
            .get();

        logger.debug("Stitching {} with {}",
            storageStorage.getDisplayName(), hypervisorStorage.getDisplayName());

        resultBuilder
            // Remove the storage-probe discovered storage
            .queueEntityRemoval(storageStorage)
            // Remove the hypervisor-probe discovered disk array
            .queueEntityRemoval(hypervisorDiskArray)
            // Update the commodities bought on the hypervisor storage to buy from the
            // storage-probe discovered disk array
            .queueChangeRelationships(hypervisorStorage,
                toUpdate -> CopyCommodities.copyCommodities().from(storageStorage).to(toUpdate));
    }

    @Nonnull
    @Override
    public StitchingIndex<List<String>, List<String>> createIndex(final int expectedSize) {
        return new StorageStitchingIndex(expectedSize);
    }

    /**
     * An index that permitting match identification for the external name lists of storages.
     * The rule for identifying a storage match by external name is as follows:
     *
     * For two storages with lists of external names, treat those lists as sets and intersect
     * them. If the intersection is empty, it is not a match. If the intersection is empty,
     * it is not a match.
     *
     * This index maintains a map of each external name to the entire list.
     */
    public static class StorageStitchingIndex implements StitchingIndex<List<String>, List<String>> {

        private final Multimap<String, List<String>> index;

        public StorageStitchingIndex(final int expectedSize) {
            index = Multimaps.newListMultimap(new HashMap<>(expectedSize), ArrayList::new);
        }

        @Override
        public void add(@Nonnull List<String> internalSignature) {
            internalSignature.forEach(externalName -> index.put(externalName, internalSignature));
        }

        @Override
        public Stream<List<String>> findMatches(@Nonnull List<String> externalSignature) {
            return externalSignature.stream()
                .flatMap(partnerExternalName -> index.get(partnerExternalName).stream());
        }
    }
}
