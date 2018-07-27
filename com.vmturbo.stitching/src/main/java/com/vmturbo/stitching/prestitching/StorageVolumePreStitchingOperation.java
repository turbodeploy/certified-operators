package com.vmturbo.stitching.prestitching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * This pre stitching operation is used to update externalNames of discovered storages based on
 * externalNames discovered on StorageVolumes by probes.
 *
 * It first creates a signature map (key is externalName, value is a set of discovered storage
 * entities) and use it to find those storages which have common externalNames with StorageVolumes.
 * Then it complements externalNames of those storages with the externalNames discovered on
 * StorageVolumes.
 *
 * Example:
 *     HyperV: ST       (externalNames: 100)
 *     Vplex:  SV       (externalNames: 100, 1001)
 *
 * Then the signature map will be:
 *     100 -> [ST]
 *
 * And 1001 will be pushed into externalNames of "ST", which will be [100, 1001].
 *
 * Note: Currently this is only used by Vplex probe, but it can also be used for other probes which
 * provide StorageVolume in the future.
 */
public class StorageVolumePreStitchingOperation implements PreStitchingOperation {

    private final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.containsAllEntityTypesScope(
                ImmutableList.of(EntityType.STORAGE_VOLUME, EntityType.STORAGE));
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(
            @Nonnull Stream<StitchingEntity> entities,
            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // list for storing all storage volumes
        List<StitchingEntity> storageVolumes = Lists.newArrayList();

        // signature map with mapping from signature to list of storages
        Multimap<String, StitchingEntity> signatureToStoragesMap = Multimaps.newSetMultimap(
                new HashMap<>(), HashSet::new);

        entities.forEach(stitchingEntity -> {
            EntityType entityType = stitchingEntity.getEntityType();
            EntityDTO.Builder builder = stitchingEntity.getEntityBuilder();
            if (entityType == EntityType.STORAGE_VOLUME) {
                storageVolumes.add(stitchingEntity);
            } else {
                // EntityType is STORAGE for this case
                if (builder.getOrigin() == EntityOrigin.DISCOVERED && builder.hasStorageData()) {
                    builder.getStorageData().getExternalNameList().forEach(externalName ->
                            signatureToStoragesMap.put(externalName, stitchingEntity));
                }
            }
        });

        // go through each StorageVolume and update related storage
        int numUpdatedStorages = 0;
        for (StitchingEntity storageVolume : storageVolumes) {
            EntityDTO.Builder svBuilder = storageVolume.getEntityBuilder();
            if (!svBuilder.hasStorageData()) {
                continue;
            }

            // find the storage which have common externalNames with this StorageVolume and
            // update its externalNames
            List<String> externalNamesList = svBuilder.getStorageData().getExternalNameList();
            List<StitchingEntity> storages = externalNamesList.stream()
                    .flatMap(externalName -> signatureToStoragesMap.get(externalName).stream())
                    .distinct().collect(Collectors.toList());
            if (storages.size() == 0) {
                continue;
            }
            if (storages.size() > 1) {
                logger.warn("Found more than 1 storages {} to update external names using storage" +
                        " volume {}. Only update for first storage.", storages, storageVolume);
            }
            resultBuilder.queueUpdateEntityAlone(storages.get(0), storage ->
                    updateExternalName(storage, externalNamesList));
            numUpdatedStorages++;
        }

        logger.debug("{} storage(s) updated with StorageVolume mapping externalNames", numUpdatedStorages);

        // remove all storage volumes
        storageVolumes.forEach(resultBuilder::queueEntityRemoval);

        return resultBuilder.build();
    }

    /**
     * Update externalNames for storage by adding new externalNames to existing ones.
     *
     * @param storage the storage whose externalNames are to be updated
     * @param externalNames new externalNames which needs to be pushed into existing list
     */
    private void updateExternalName(@Nonnull StitchingEntity storage,
                                    @Nonnull List<String> externalNames) {
        final Set<String> newExternalNames = new HashSet<>();
        newExternalNames.addAll(storage.getEntityBuilder().getStorageData().getExternalNameList());
        newExternalNames.addAll(externalNames);
        storage.getEntityBuilder().getStorageDataBuilder().clearExternalName();
        storage.getEntityBuilder().getStorageDataBuilder().addAllExternalName(newExternalNames);
    }
}
