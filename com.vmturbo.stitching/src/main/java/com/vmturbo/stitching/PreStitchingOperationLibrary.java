package com.vmturbo.stitching;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.prestitching.ConnectedNetworkPreStitchingOperation;
import com.vmturbo.stitching.prestitching.MergeSharedDatacentersPreStitchingOperation;
import com.vmturbo.stitching.prestitching.RemoveNonMarketEntitiesPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedCloudEntityPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedStoragePreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedVirtualVolumePreStitchingOperation;
import com.vmturbo.stitching.prestitching.StorageVolumePreStitchingOperation;

/**
 * A library of {@link PreStitchingOperation}s. Maintains the known topology preStitching operations
 * so that they can be run at the appropriate phases of the stitching lifecycle.
 *
 * {@link PreStitchingOperation}s are maintained in the order that they are run.
 */
@Immutable
public class PreStitchingOperationLibrary {
    private final ImmutableList<PreStitchingOperation> preStitchingOperations;
    /**
     * Entity types from AWS that need to be merged.
     */
    private static final List<EntityType> AWS_ENTITY_TYPES = ImmutableList.of(
        EntityType.AVAILABILITY_ZONE,
        EntityType.CLOUD_SERVICE,
        EntityType.COMPUTE_TIER,
        EntityType.DATABASE_SERVER_TIER,
        EntityType.REGION,
        EntityType.STORAGE_TIER);

    /**
     * Entity types from Azure that need to be merged.
     */
    private static final List<EntityType> AZURE_ENTITY_TYPES = ImmutableList.of(
        EntityType.CLOUD_SERVICE,
        EntityType.COMPUTE_TIER,
        EntityType.DATABASE_TIER,
        EntityType.REGION,
        EntityType.RESERVED_INSTANCE,
        EntityType.STORAGE_TIER);


    /**
     * Create a new {@link PreStitchingOperation} library.
     */
    public PreStitchingOperationLibrary() {
        ImmutableList.Builder<PreStitchingOperation> listBuilder = new ImmutableList.Builder<>();
        listBuilder.addAll(createCloudEntityPreStitchingOperations());
        preStitchingOperations = listBuilder.add(
                new RemoveNonMarketEntitiesPreStitchingOperation(),
                new SharedStoragePreStitchingOperation(),
                new MergeSharedDatacentersPreStitchingOperation(),
                new StorageVolumePreStitchingOperation(),
                new SharedVirtualVolumePreStitchingOperation(),
                new ConnectedNetworkPreStitchingOperation()
        )
        .build();
    }

    private static Collection<PreStitchingOperation> createCloudEntityPreStitchingOperations() {
        List<PreStitchingOperation> operationList =
            AWS_ENTITY_TYPES.stream()
                .map(entityType ->
                    new SharedCloudEntityPreStitchingOperation(SDKProbeType.AWS.getProbeType(),
                        entityType))
                .collect(Collectors.toList());
        AZURE_ENTITY_TYPES.forEach(entityType -> operationList.add(
            new SharedCloudEntityPreStitchingOperation(SDKProbeType.AZURE.getProbeType(),
                entityType)));
        return operationList;
    }

    /**
     * Get the list of {@link PreStitchingOperation} to run prior to the main {@link StitchingOperation}s.
     *
     * @return the list of {@link PreStitchingOperation} to run prior to the main {@link StitchingOperation}s.
     */
    public List<PreStitchingOperation> getPreStitchingOperations() {
        return preStitchingOperations;
    }
}
