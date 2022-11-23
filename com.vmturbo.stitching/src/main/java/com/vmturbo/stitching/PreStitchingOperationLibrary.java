package com.vmturbo.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SessionData;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.prestitching.ADGroupsPreStitchingOperation;
import com.vmturbo.stitching.prestitching.AwsBusinessAccountPreStitchingOperation;
import com.vmturbo.stitching.prestitching.CloudCommitmentPreStitchingOperation;
import com.vmturbo.stitching.prestitching.ConnectedNetworkPreStitchingOperation;
import com.vmturbo.stitching.prestitching.ContainerClusterPreStitchingOperation;
import com.vmturbo.stitching.prestitching.RemoveNonMarketEntitiesPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedCloudEntityPreStitchingOperation;
import com.vmturbo.stitching.prestitching.SharedEntityCustomProbePreStitchingOperation;
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
     * Entity types from AWS that need to be merged, merge properties boolean.
     */
    private static final Map<EntityType, Boolean> AWS_ENTITY_TYPES =
            ImmutableMap.<EntityType, Boolean>builder()
                    .put(EntityType.SERVICE_PROVIDER, false)
                    .put(EntityType.CLOUD_SERVICE, false)
                    .put(EntityType.COMPUTE_TIER, false)
                    .put(EntityType.DATABASE_SERVER_TIER, false)
                    .put(EntityType.STORAGE_TIER, false)
                    .put(EntityType.REGION, false)
                    .put(EntityType.AVAILABILITY_ZONE, false)
                    .put(EntityType.CLOUD_COMMITMENT, true)
                    .build();

    /**
     * Entity types from Azure that need to be merged, merge properties boolean.
     */
    public static final Map<EntityType, Boolean> AZURE_ENTITY_TYPES =
            ImmutableMap.<EntityType, Boolean>builder()
                    .put(EntityType.SERVICE_PROVIDER, false)
                    .put(EntityType.CLOUD_SERVICE, false)
                    // there is some difference in properties between targets for azure compute tiers
                    // like: some return [standardA0_A7Family] while some return
                    // [standardA0_A7Family, Standard A0-A7 Family vCPUs]
                    .put(EntityType.COMPUTE_TIER, true)
                    .put(EntityType.DATABASE_TIER, false)
                    .put(EntityType.STORAGE_TIER, false)
                    // merge all properties of Azure Regions that are shared by more than one Azure
                    // Subscription target. Azure Subscription targets send CPU quota information
                    // in entity properties of EntityDTOs representing Regions. Since Regions are
                    // shared by Azure targets and each target sends quotas for one subscription
                    // only, we need to merge properties of Region entities so that resulting
                    // entity represent quotas for all subscriptions.
                    .put(EntityType.REGION, true)
                    .put(EntityType.RESERVED_INSTANCE, false)
                    .build();

    /**
     * Entity types from GCP that need to be merged, merge properties boolean.
     */
    private static final Map<EntityType, Boolean> GCP_ENTITY_TYPES =
            ImmutableMap.<EntityType, Boolean>builder()
                    .put(EntityType.SERVICE_PROVIDER, false)
                    .put(EntityType.CLOUD_SERVICE, false)
                    .put(EntityType.COMPUTE_TIER, false)
                    .put(EntityType.STORAGE_TIER, false)
                    .put(EntityType.REGION, false)
                    .put(EntityType.AVAILABILITY_ZONE, false)
                    .build();

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
                new ADGroupsPreStitchingOperation(),
                new SharedEntityDefaultPreStitchingOperation(
                        stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                                SDKProbeType.VMWARE_HORIZON_VIEW.getProbeType(),
                                EntityType.BUSINESS_USER), Collections.singletonMap(
                        "common_dto.EntityDTO.BusinessUserData.sessionData",
                        Comparator.comparing(lhs -> ((SessionData)lhs).getVirtualMachine()))),
                new SharedEntityCustomProbePreStitchingOperation(),
                new AwsBusinessAccountPreStitchingOperation(),
                // This operation should go after SharedCloudEntityPreStitchingOperation because it
                // depends on merging shared Regions.
                new CloudCommitmentPreStitchingOperation(),
                new ContainerClusterPreStitchingOperation())
            .build();
    }

    private static Collection<PreStitchingOperation> createCloudEntityPreStitchingOperations() {
        final Collection<PreStitchingOperation> operations = AWS_ENTITY_TYPES.entrySet().stream()
                .map(entry -> createCloudEntityPreStitchingOperation(
                        ImmutableSet.of(SDKProbeType.AWS), entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        AZURE_ENTITY_TYPES.forEach((entityType, mergeProperties) -> operations.add(
                createCloudEntityPreStitchingOperation(
                        ImmutableSet.of(SDKProbeType.AZURE, SDKProbeType.AZURE_INFRA), entityType, mergeProperties)));
        GCP_ENTITY_TYPES.forEach((entityType, mergeProperties) -> operations.add(
                createCloudEntityPreStitchingOperation(
                        ImmutableSet.of(SDKProbeType.GCP_PROJECT), entityType, mergeProperties)));
        return operations;
    }

    /**
     * Create a CloudEntityPreStitchingOperation for the parameters.
     *
     * @param probeType probe type
     * @param entityType type of entity to merge
     * @param mergeProperties whether to merge properties
     * @return CloudEntityPreStitchingOperation
     */
    public static PreStitchingOperation createCloudEntityPreStitchingOperation(
            Set<SDKProbeType> probeTypes, EntityType entityType, boolean mergeProperties) {

        final Set<String> probeNames = probeTypes.stream()
                .map(SDKProbeType::getProbeType)
                .collect(ImmutableSet.toImmutableSet());
        return new SharedCloudEntityPreStitchingOperation(
                stitchingScopeFactory -> stitchingScopeFactory.multiProbeEntityTypeScope(
                        probeNames, entityType), mergeProperties);
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
