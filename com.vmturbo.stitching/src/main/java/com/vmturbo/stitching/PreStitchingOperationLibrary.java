package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SessionData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.prestitching.ConnectedNetworkPreStitchingOperation;
import com.vmturbo.stitching.prestitching.RemoveNonMarketEntitiesPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedCloudEntityPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedEntityDefaultPreStitchingOperation;
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
                new SharedEntityDefaultPreStitchingOperation(
                        stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                                SDKProbeType.HYPERV.getProbeType(), EntityType.DATACENTER)),
                new StorageVolumePreStitchingOperation(),
                new SharedVirtualVolumePreStitchingOperation(),
                new ConnectedNetworkPreStitchingOperation(),
                new SharedEntityDefaultPreStitchingOperation(
                        stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                                SDKProbeType.VMWARE_HORIZON_VIEW.getProbeType(),
                                EntityType.BUSINESS_USER), Collections.singletonMap(
                        "common_dto.EntityDTO.BusinessUserData.sessionData",
                        Comparator.comparing(lhs -> ((SessionData)lhs).getVirtualMachine()))))
                .build();
    }

    private static Collection<PreStitchingOperation> createCloudEntityPreStitchingOperations() {
        final Collection<PreStitchingOperation> operations = AWS_ENTITY_TYPES.stream()
                .map(entityType -> new SharedCloudEntityPreStitchingOperation(
                        stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                                SDKProbeType.AWS.getProbeType(), entityType)))
                .collect(Collectors.toList());
        AZURE_ENTITY_TYPES.forEach(entityType -> operations.add(
                new SharedCloudEntityPreStitchingOperation(
                        stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                                SDKProbeType.AZURE.getProbeType(), entityType))));
        return operations;
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
