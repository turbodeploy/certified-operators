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
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.MergeEntities;

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
 * - Connect hypervisor storage to storage disk array or logical pool using commodities from the
 *   storage storage.
 *
 *      Hypervisor    Storage
 *
 *         App
 *          |
 *         VM
 *        /  \   connect
 *       PM  ST--------* ST <--delete
 *       |    |         \ |
 *       DC  DA<-delete  DA/LP
 *                        |
 *                       SC
 */
public class StorageStitchingOperation implements StitchingOperation<List<String>, List<String>> {
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>> getScope(
            @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return Optional.empty();
    }

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
    public TopologicalChangelog<StitchingEntity> stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                                        @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        stitchingPoints.forEach(stitchingPoint -> stitch(stitchingPoint, resultBuilder));

        return resultBuilder.build();
    }

    /**
     * Merge the hypervisor-probe disk array onto the storage-probe disk array/logical pool.
     * Merge the storage-probe storage onto the hypervisor-probe disk array.
     * Connect the hypervisor-probe storage to the storage-probe disk array/logical pool.
     *
     * @param stitchingPoint The point at which the storage graph should be stitched with
     *                       the graphs discovered by external probes.
     * @param resultBuilder The builder of the results of the stitching operation. Changes to
     *                      relationships made by the stitching operation should be noted
     *                      in these results.
     */
    private void stitch(@Nonnull final StitchingPoint stitchingPoint,
                        @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // The storage and disk array discovered by the storage probe
        final StitchingEntity storageStorage = stitchingPoint.getInternalEntity();
        final StitchingEntity storageDiskArrayOrLogicalPool = storageStorage.getProviders().stream()
            .filter(entity -> entity.getEntityType() == EntityType.DISK_ARRAY ||
                    entity.getEntityType() == EntityType.LOGICAL_POOL)
            .findFirst()
            .get();

        // The storage and disk array or logical pool discovered by the hypervisor probe
        final StitchingEntity hypervisorStorage = stitchingPoint.getExternalMatches().iterator().next();
        final StitchingEntity hypervisorDiskArray = hypervisorStorage.getProviders().stream()
            .filter(entity -> entity.getEntityType() == EntityType.DISK_ARRAY)
            .findFirst()
            .get();

        logger.debug("Stitching {} with {}",
            storageStorage.getDisplayName(), hypervisorStorage.getDisplayName());

        resultBuilder
            // All the commodities bought by the storage storage should now be bought by the hypervisor storage.
            .queueChangeRelationships(hypervisorStorage, toUpdate -> {
                toUpdate.removeProvider(hypervisorDiskArray);
                CopyCommodities.copyCommodities().from(storageStorage).to(toUpdate);
            })
            // Merge the storage-probe discovered storage onto the hypervisor probe discovered storage.
            .queueEntityMerger(MergeEntities.mergeEntity(storageStorage).onto(hypervisorStorage))
            // Merge the hypervisor diskArray onto the storage DiskArray.
            .queueEntityMerger(MergeEntities.mergeEntity(hypervisorDiskArray)
                    .onto(storageDiskArrayOrLogicalPool));
    }

    @Nonnull
    @Override
    public StitchingIndex<List<String>, List<String>> createIndex(final int expectedSize) {
        return new StorageStitchingIndex(expectedSize);
    }

    /**
     * An index that permits matching for the external name lists of storages.
     * The rule for identifying a storage match by external name is as follows:
     *
     * For two storages with lists of external names, treat those lists as sets and intersect
     * them. If the intersection is empty, it is not a match. If the intersection is non-empty,
     * it is a match.
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
