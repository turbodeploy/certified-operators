package com.vmturbo.topology.processor.conversions.cloud;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityStore.TargetStitchingDataMap;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Convert cloud topology entities.
 *
 * NOTE:
 * This class contains a lot of entity conversion functionality that we would prefer to be in the
 * cloud probe itself. For example, the cloud probes are sending fake Physical Machine and DataCenter
 * objects that represent AvailabilityZone and Region entities. This class handles converting these
 * mock cloud entities to the new entity types we feel more directly model the cloud entity system.
 *
 * Because we do not want to disrupt functionality in Ops Manager, we are, hopefully temporarily,
 * doing these translations here. In the future, we want to change the cloud probes to directly model
 * using the new entity types instead.
 */
public class CloudTopologyConverter {

    private static final Logger logger = LogManager.getLogger();

    // TODO: remove this list and switch to using probe category when OM-31984 is addressed.
    public static final List<SDKProbeType> CLOUD_PROBE_TYPES = Arrays.asList(
            SDKProbeType.AWS, SDKProbeType.AZURE);

    private EntityStore entityStore;

    private TargetStore targetStore;

    private IdentityProvider identityProvider;

    private final CloudEntityRewirer cloudEntityRewirer;

    private static Map<EntityType, EntityType> PROFILE_TYPE_TO_CLOUD_ENTITY_TYPE = ImmutableMap.of(
            EntityType.VIRTUAL_MACHINE, EntityType.COMPUTE_TIER,
            EntityType.DATABASE,        EntityType.DATABASE_TIER,
            EntityType.DATABASE_SERVER, EntityType.DATABASE_TIER
    );

    public static String AWS_EC2 = "AWS EC2";
    public static String AWS_RDS = "AWS RDS";
    public static String AWS_EBS = "AWS EBS";
    public static String AZURE_VIRTUAL_MACHINES = "Azure VirtualMachines";
    public static String AZURE_DATA_SERVICES = "Azure DataServices";
    public static String AZURE_STORAGE = "Azure Storage";

    // todo: these cloud services are hardcoded for now, we may want to use data from probe
    private static Map<SDKProbeType, List<String>> CLOUD_SERVICES_BY_PROBE_TYPE = ImmutableMap.of(
            SDKProbeType.AWS, Lists.newArrayList(AWS_EC2, AWS_RDS, AWS_EBS),
            SDKProbeType.AZURE, Lists.newArrayList(AZURE_VIRTUAL_MACHINES, AZURE_DATA_SERVICES, AZURE_STORAGE)
    );

    // table showing which EntityType to be owned by which CloudService
    public static final ImmutableTable<SDKProbeType, EntityType, String> ENTITY_TYPE_OWNED_BY_CLOUD_SERVICE_TABLE =
            new ImmutableTable.Builder<SDKProbeType, EntityType, String>()
                    .put(SDKProbeType.AWS, EntityType.COMPUTE_TIER, AWS_EC2)
                    .put(SDKProbeType.AWS, EntityType.DATABASE_TIER, AWS_RDS)
                    .put(SDKProbeType.AWS, EntityType.STORAGE_TIER, AWS_EBS)
                    .put(SDKProbeType.AZURE, EntityType.COMPUTE_TIER, AZURE_VIRTUAL_MACHINES)
                    .put(SDKProbeType.AZURE, EntityType.DATABASE_TIER, AZURE_DATA_SERVICES)
                    .put(SDKProbeType.AZURE, EntityType.STORAGE_TIER, AZURE_STORAGE)
                    .build();

    private final Table<SDKProbeType, EntityType, Map<String, StitchingEntityData>> sharedEntities
            = HashBasedTable.create();

    // keep BusinessAccounts for AWS and Azure, so they can be used in rewire step
    private final Map<Long, Set<String>> businessAccountLocalIdsByTargetId = new HashMap<>();

    public CloudTopologyConverter(EntityStore entityStore,
                                  TargetStore targetStore,
                                  IdentityProvider identityProvider,
                                  CloudEntityRewirer cloudEntityRewirer) {
        this.entityStore = entityStore;
        this.targetStore = targetStore;
        this.identityProvider = identityProvider;
        this.cloudEntityRewirer = cloudEntityRewirer;
    }

    /**
     * Given an entity, return a unique oid for it, if possible.
     * @return an Optional id.
     */
    private Optional<Long> getUniqueOidForEntity(EntityDTO entity, long targetOid) {
        // get an oid for this new entity from the identityProvder.
        try {
            // get the probe id and use that get an entity oid. Throw an exception
            // if the probe id is not found.
            Optional<Long> optionalProbeId = targetStore.getProbeIdForTarget(targetOid);
            long oid = identityProvider.getIdForEntity(
                    optionalProbeId.orElseThrow(
                            () -> new RuntimeException("Could not find probe for target "+ targetOid)),
                    entity
            );
            return Optional.of(oid);
        } catch (IdentityMetadataMissingException | IdentityProviderException |
                IdentityUninitializedException | RuntimeException e) {
            logger.error("Error getting oid for target {} entity {}:",
                    targetOid, entity.getDisplayName(), e);
        }
        return Optional.empty();
    }

    /**
     * Create new cloud entities based on data from cloud probe. Currently, ComputeTier and
     * DatabaseTier, and CloudService are created.
     *
     * We should move this behavior into the cloud probes when we have the opportunity to refactor
     * the way the probe works.
     *
     * @param targetIdToEntityProfiles a map of targetId -> set of EntityProfileDTOs discovered by each target.
     * @return a list of {@link StitchingEntityData}s for any new cloud entities
     */
    public List<StitchingEntityData> createNewCloudEntities(
            @Nonnull Map<Long, Set<EntityProfileDTO>> targetIdToEntityProfiles) {
        List<StitchingEntityData> stitchingEntityDataList = new ArrayList<>();
        stitchingEntityDataList.addAll(createServiceTiersFromProfiles(targetIdToEntityProfiles));
        stitchingEntityDataList.addAll(createCloudServices());
        return stitchingEntityDataList;
    }

    /**
     * We need to create service tier entities, from {@link EntityProfileDTO} objects. Currently,
     * ComputeTier and DatabaseTier are created.
     *
     * We should move this behavior into the cloud probes when we have the opportunity to refactor
     * the way the probe works.
     *
     * @param targetIdToEntityProfiles a map of targetId -> set of EntityProfileDTOs discovered by each target.
     * @return a list of {@link StitchingEntityData}s for any entities created from the profiles.
     */
    public List<StitchingEntityData> createServiceTiersFromProfiles(
            Map<Long, Set<EntityProfileDTO>> targetIdToEntityProfiles) {
        List<StitchingEntityData> stitchingEntityDataList = new ArrayList<>();

        // create any entities needed based on the cloud-related profiles.
        targetIdToEntityProfiles.forEach((targetOid, profiles) -> {
            Optional<SDKProbeType> optionalProbeType = targetStore.getProbeTypeForTarget(targetOid);
            if (!optionalProbeType.isPresent()) {
                return;
            }

            SDKProbeType probeType = optionalProbeType.get();
            if (!CLOUD_PROBE_TYPES.contains(optionalProbeType.get())) {
                return;
            }

            profiles.stream()
                    .filter(entityProfileDTO -> !alreadyCreated(entityProfileDTO.getId(), probeType,
                            PROFILE_TYPE_TO_CLOUD_ENTITY_TYPE.get(entityProfileDTO.getEntityType())))
                    .forEach(entityProfileDTO -> {

                EntityDTO.Builder builder = createEntityDTOFromProfile(entityProfileDTO);
                if (builder == null) { // nothing to convertAndAddToStitchingGraph this time
                    return;
                }
                // get an oid for this new entity from the identityProvder.
                getUniqueOidForEntity(builder.buildPartial(), targetOid).ifPresent(entityOid -> {
                    // we are adding a new entity -- create the StitchingEntityData for it.
                    StitchingEntityData ct = StitchingEntityData.newBuilder(builder)
                            .profile(entityProfileDTO)
                            .oid(entityOid)
                            .targetId(targetOid)
                            .lastUpdatedTime(entityStore.getTargetLastUpdatedTime(targetOid).orElse(0L))
                            .probeType(probeType)
                            .build();

                    stitchingEntityDataList.add(ct);

                    // add to shared entities
                    sharedEntities.get(probeType, builder.getEntityType()).put(entityProfileDTO.getId(), ct);
                });
            });
        });

        return stitchingEntityDataList;
    }

    /**
     * Create cloud services based on the table {@link CloudTopologyConverter#CLOUD_SERVICES_BY_PROBE_TYPE},
     * and then put into sharedEntities table for use by rewirers later.
     */
    public List<StitchingEntityData> createCloudServices() {
        List<StitchingEntityData> stitchingEntityDataList = new ArrayList<>();

        CLOUD_SERVICES_BY_PROBE_TYPE.forEach((probeType, cloudServices) -> targetStore.getAll().stream()
            .filter(target -> {
                Optional<SDKProbeType> optionalProbeType = targetStore.getProbeTypeForTarget(target.getId());
                return optionalProbeType.isPresent() && optionalProbeType.get() == probeType;
            })
            .findAny()
            .ifPresent(target -> cloudServices.forEach(cloudService -> {
                if (alreadyCreated(cloudService, probeType, EntityType.CLOUD_SERVICE)) {
                    return;
                }

                EntityDTO.Builder builder = EntityDTO.newBuilder();
                builder.setEntityType(EntityType.CLOUD_SERVICE);
                builder.setId(cloudService);
                builder.setDisplayName(cloudService);

                // get an oid for this new entity from the identityProvder
                Long targetId = target.getId();
                Optional<Long> optionalOid = getUniqueOidForEntity(builder.buildPartial(), targetId);
                if (optionalOid.isPresent()) {
                    StitchingEntityData cs = StitchingEntityData.newBuilder(builder)
                            .oid(optionalOid.get())
                            .targetId(targetId)
                            .lastUpdatedTime(entityStore.getTargetLastUpdatedTime(targetId).orElse(0L))
                            .probeType(probeType)
                            .build();
                    stitchingEntityDataList.add(cs);
                    sharedEntities.get(probeType, EntityType.CLOUD_SERVICE).put(cloudService, cs);
                }
            }))
        );

        return stitchingEntityDataList;
    }

    /**
     * Check if the entity for the given local id is created in the sharedEntities table, if no
     * entry for the probe type and entity type, then create a new empty map and put in the table.
     */
    private boolean alreadyCreated(String localId, SDKProbeType probeType, EntityType entityType) {
        Map<String, StitchingEntityData> map = sharedEntities.get(probeType, entityType);
        if (map == null) {
            sharedEntities.put(probeType, entityType, new HashMap<>());
            return false;
        }
        return map.containsKey(localId);
    }

    /**
     * Create StorageTier entity based on the set of cloud storages discovered.
     *
     * @param storageBuilder the dto builder of the storage to create storage tier for
     * @param probeType the probe type of the target this storage dto comes from
     * @param targetOid oid of the target which contains this storage dto
     * @return a list of {@link StitchingEntityData} representing the storage tiers.
     */
    private Optional<StitchingEntityData> createStorageTier(EntityDTO.Builder storageBuilder,
            SDKProbeType probeType, Long targetOid) {
        // we got one -- create a storage tier based on it's storage data
        if (!storageBuilder.hasStorageData()) {
            return Optional.empty();
        }

        String storageTier = storageBuilder.getStorageData().getStorageTier();
        if (alreadyCreated(storageTier, probeType, EntityType.STORAGE_TIER)) {
            return Optional.empty();
        }

        // create the entity for it.
        EntityDTO.Builder builder = EntityDTO.newBuilder();
        builder.setId(storageTier);
        builder.setDisplayName(storageTier);
        builder.setEntityType(EntityType.STORAGE_TIER);
        // get an oid for this new entity from the identityProvder.
        Optional<Long> optionalOid = getUniqueOidForEntity(builder.buildPartial(), targetOid);
        if (optionalOid.isPresent()) {
            // we have a new UNIQUE storage tier
            StitchingEntityData st = StitchingEntityData.newBuilder(builder)
                    .oid(optionalOid.get())
                    .targetId(targetOid)
                    .lastUpdatedTime(entityStore.getTargetLastUpdatedTime(targetOid).orElse(0L))
                    .probeType(probeType)
                    .build();
            sharedEntities.get(probeType, EntityType.STORAGE_TIER).put(storageTier, st);
            return Optional.of(st);
        }
        return Optional.empty();
    }

    /**
     * Given an {@link Entity}, create one or many {@link StitchingEntityData} objects to represent
     * the data in that entity.
     *
     * Normally one {@link StitchingEntityData} would be created for an entity, but we may in some
     * cases create multiple StitchingEntityData objects, such as when the same entity is
     * discovered by multiple targets.
     *
     * Note that all entity types we are including in the switch case will be flagged as "cloud"
     * entities and will be passed through the cloud editing process later in the stitching
     * context setup. So include any entity types that not only need direct editing, but may need
     * to be reconnected or have commodity changes later.
     *
     * @param entityBuilder the probe entity builder to adjust
     */
    public void convertCloudEntity(EntityDTO.Builder entityBuilder, Long entityOid, Long targetId,
            SDKProbeType probeType, Long targetLastUpdatedTime, TargetStitchingDataMap stitchingDataMap) {
        // convert the fake cloud entity types to the new "explicit" cloud entity types we want
        // to use going forward.
        EntityType entityType = entityBuilder.getEntityType();

        if (entityType == EntityType.PHYSICAL_MACHINE) {
            entityBuilder.setEntityType(EntityType.AVAILABILITY_ZONE);
        } else if (entityType == EntityType.DATACENTER) {
            entityBuilder.setEntityType(EntityType.REGION);
        } else if (entityType == EntityType.STORAGE) {
            // create StorageTier and put into sharedEntities table
            createStorageTier(entityBuilder, probeType, targetId).ifPresent(st -> stitchingDataMap.put(st));
        } else if (entityType == EntityType.BUSINESS_ACCOUNT) {
            // keep BusinessAccount so we can set owns relationship later
            businessAccountLocalIdsByTargetId.computeIfAbsent(targetId, k -> new HashSet<>()).add(
                    entityBuilder.getId());
        }

        StitchingEntityData stitchingEntityData = StitchingEntityData.newBuilder(entityBuilder)
                .oid(entityOid)
                .targetId(targetId)
                .probeType(probeType)
                .lastUpdatedTime(targetLastUpdatedTime)
                .build();
        stitchingDataMap.put(stitchingEntityData);

        // add to shared entities table for AZ and Region
        if (entityBuilder.getEntityType() == EntityType.AVAILABILITY_ZONE ||
                entityBuilder.getEntityType() == EntityType.REGION) {
            addToSharedEntities(probeType, entityBuilder.getEntityType(), stitchingEntityData);
        }
    }

    private void addToSharedEntities(SDKProbeType probeType, EntityType entityType,
            StitchingEntityData stitchingEntityData) {
        Map<String, StitchingEntityData> entitiesByLocalId = sharedEntities.get(probeType, entityType);
        if (entitiesByLocalId == null) {
            entitiesByLocalId = new HashMap<>();
            entitiesByLocalId.put(stitchingEntityData.getLocalId(), stitchingEntityData);
            sharedEntities.put(probeType, entityType, entitiesByLocalId);
        } else {
            entitiesByLocalId.putIfAbsent(stitchingEntityData.getLocalId(), stitchingEntityData);
        }
    }

    /**
     * Convert profile to ComputeTier, for following types of profiles:
     *     AWS:    VIRTUAL_MACHINE -> COMPUTE_TIER
     *             DATABASE_SERVER -> DATABASE_TIER
     *     Azure:  VIRTUAL_MACHINE -> COMPUTE_TIER
     *             DATABASE        -> DATABASE_TIER
     *
     * @param entityProfileDTO the EntityProfileDTO which needs to be converted to ComputeTier
     * @return ComputeTier EntityDTO created from the given profile dto
     */
    private EntityDTO.Builder createEntityDTOFromProfile(EntityProfileDTO entityProfileDTO) {
        EntityDTO.Builder builder = EntityDTO.newBuilder();
        builder.setId(entityProfileDTO.getId());
        builder.setDisplayName(entityProfileDTO.getDisplayName());

        EntityType newCloudEntityType = PROFILE_TYPE_TO_CLOUD_ENTITY_TYPE.get(
                entityProfileDTO.getEntityType());
        if (newCloudEntityType == null) {
            // don't convert, but log it so we can see what we're missing.
            logger.info("Skipping entity profile {} of type {}", builder.getDisplayName(),
                    entityProfileDTO.getEntityType().name());
            return null;
        }

        builder.setEntityType(newCloudEntityType);
        return builder;
    }

    /**
     * Get region local id based on AZ local id. For example:
     *     AWS AZ id is:     aws::ca-central-1::PM::ca-central-1b
     *     AWS Region id is: aws::ca-central-1::DC::ca-central-1
     * It get "ca-central-1" from AZ id and then combine with prefix into the Region id
     *
     * @param azLocalId local id of the AZ
     * @param probeType the probe type from which this AZ comes
     * @return region local id for the AZ
     */
    public static String getRegionLocalId(String azLocalId, SDKProbeType probeType) {
        String region = azLocalId.split("::",3)[1];
        String prefix = probeType == SDKProbeType.AWS ? "aws::" : "azure::";
        return prefix + region + "::DC::" + region;
    }

    /**
     * Rewire the given TopologyStitchingEntity.
     *
     * @param entity the {@link TopologyStitchingEntity} to rewire
     * @param entitiesByLocalId
     * @param stitchingGraph
     * @return true, if the entity should be removed.
     */
    public boolean rewire(
            @Nonnull final TopologyStitchingEntity entity,
            @Nonnull final Map<String, StitchingEntityData> entitiesByLocalId,
            @Nonnull final TopologyStitchingGraph stitchingGraph) {
        // call the rewiring helper for this entity
        return cloudEntityRewirer.rewire(entity, entitiesByLocalId, stitchingGraph,
                businessAccountLocalIdsByTargetId, sharedEntities);
    }
}