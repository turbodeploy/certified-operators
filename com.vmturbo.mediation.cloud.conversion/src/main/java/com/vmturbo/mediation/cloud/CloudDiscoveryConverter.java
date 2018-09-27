package com.vmturbo.mediation.cloud;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.mediation.cloud.converter.DefaultConverter;
import com.vmturbo.mediation.cloud.util.CloudService;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;

/**
 * Convert cloud discovery response (entities, profiles...) to new cloud model for use by XL.
 *
 * This class contains a lot of entity conversion functionality that we would prefer to be in the
 * cloud probe itself. For example, the cloud probes are sending fake Physical Machine and DataCenter
 * objects that represent AvailabilityZone and Region entities. This class handles converting these
 * mock cloud entities to the new entity types we feel more directly model the cloud entity system.
 *
 * Because we do not want to disrupt functionality in Ops Manager, we are, hopefully temporarily,
 * doing these translations here. In the future, we want to change the cloud probes to directly model
 * using the new entity types instead.
 */
public class CloudDiscoveryConverter {

    private final Logger logger = LogManager.getLogger();

    private static final IEntityConverter defaultConverter = new DefaultConverter();

    private Map<String, EntityDTO> rawEntityDTOsById = new HashMap<>();

    private Map<String, EntityDTO.Builder> newEntityBuildersById = new HashMap<>();

    private Map<String, EntityProfileDTO> profileDTOsById = new HashMap<>();

    private Set<String> allStorageTierIds = new HashSet<>();

    // the business account which is used to own entities (VMs, Apps, etc).
    // For AWS, this is the master account if it is available, or sub account if not. For Azure,
    // there is only one account in the discovery response, which is used to own entities.
    private EntityDTO.Builder businessAccountToOwnEntities;

    private final DiscoveryResponse.Builder discoveryResponseBuilder;

    private final CloudProviderConversionContext conversionContext;

    public CloudDiscoveryConverter(@Nonnull DiscoveryResponse discoveryResponse,
                     @Nonnull CloudProviderConversionContext conversionContext) {
        this.discoveryResponseBuilder = discoveryResponse.toBuilder();
        this.conversionContext = conversionContext;
    }

    /**
     * Convert old discovery response to new discovery response in new cloud model.
     *
     * @return new discovery response
     */
    public DiscoveryResponse convert() {
        // pre process discovery response
        preProcess();
        // entity type specific convert
        entityTypeSpecificConvert();

        return discoveryResponseBuilder.build();
    }

    /**
     * Pre process the discovery response (mainly entity dtos and profile dtos) to prepare for
     * entity type specific conversion later, such as putting entities into a map by id, etc.
     */
    public void preProcess() {
        // initial loop through entities list, prepare for entity type specific converter later
        discoveryResponseBuilder.getEntityDTOList().forEach(this::preProcessEntityDTO);
        // create tiers (ComputeTier and DatabaseTier) from profiles
        discoveryResponseBuilder.getEntityProfileList().forEach(this::createEntityDTOFromProfile);
        // create cloud service entity
        // todo: convert NonMarketEntityDTO to cloud service? but not discovered for sub accounts
        createCloudServices();
    }

    /**
     * Pre process an EntityDTO to prepare for entity type specific conversion late, such as:
     * change the fake cloud entity type to the new "explicit" cloud entity type, create new
     * empty (no commodities) storage tiers, etc.
     *
     * @param entityDTO the EntityDTO to pre process
     */
    private void preProcessEntityDTO(@Nonnull EntityDTO entityDTO) {
        EntityDTO.Builder entityBuilder = entityDTO.toBuilder();
        EntityType entityType = entityBuilder.getEntityType();

        if (entityType == EntityType.PHYSICAL_MACHINE) {
            entityBuilder.setEntityType(EntityType.AVAILABILITY_ZONE);
        } else if (entityType == EntityType.DATACENTER) {
            entityBuilder.setEntityType(EntityType.REGION);
        } else if (entityType == EntityType.STORAGE) {
            createStorageTier(entityBuilder);
        } else if (entityType == EntityType.BUSINESS_ACCOUNT) {
            // for aws there are two cases, store master account if possible:
            // 1. this aws target is added using sub account (discover sub)
            // 2. this aws target is added using master account (discover master + list of subs)
            if (businessAccountToOwnEntities == null || entityBuilder.getConsistsOfCount() > 0) {
                businessAccountToOwnEntities = entityBuilder;
            }
        }

        // add to map for every entity builder
        newEntityBuildersById.put(entityBuilder.getId(), entityBuilder);
        // add original EntityDTO to map
        rawEntityDTOsById.put(entityDTO.getId(), entityDTO);
    }

    /**
     * Go through every pre-processed EntityDTO, call entity type specific converter for each
     * EntityDTO, and finally build a new discovery response with new cloud entities.
     */
    private void entityTypeSpecificConvert() {
        // entity type specific convert
        List<EntityDTO.Builder> builders = newEntityBuildersById.values().stream()
                .filter(entity -> conversionContext.getEntityConverters().getOrDefault(
                        entity.getEntityType(), defaultConverter).convert(entity, this))
                .collect(Collectors.toList());
        discoveryResponseBuilder.clearEntityDTO();
        discoveryResponseBuilder.addAllEntityDTO(builders.stream()
                .map(EntityDTO.Builder::build)
                .collect(Collectors.toList()));
    }

    /**
     * Create StorageTier entity based on the set of cloud storages discovered.
     *
     * @param storageBuilder the dto builder of the storage to create storage tier for
     */
    private void createStorageTier(@Nonnull EntityDTO.Builder storageBuilder) {
        // we got one -- create a storage tier based on it's storage data
        if (!storageBuilder.hasStorageData()) {
            return;
        }

        final String storageTier = storageBuilder.getStorageData().getStorageTier();
        final String storageTierId = conversionContext.getStorageTierId(storageTier);
        // create new StorageTier if not existing
        newEntityBuildersById.computeIfAbsent(storageTierId, k -> {
            // create the entity for it.
            EntityDTO.Builder stBuilder = EntityDTO.newBuilder();
            stBuilder.setId(storageTierId);
            stBuilder.setDisplayName(storageTier);
            stBuilder.setEntityType(EntityType.STORAGE_TIER);
            // add to set
            allStorageTierIds.add(storageTierId);
            return stBuilder;
        });
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
    private void createEntityDTOFromProfile(EntityProfileDTO entityProfileDTO) {
        String profileId = entityProfileDTO.getId();
        EntityDTO.Builder builder = EntityDTO.newBuilder();
        builder.setId(profileId);
        builder.setDisplayName(entityProfileDTO.getDisplayName());

        Optional<EntityType> newCloudEntityType = conversionContext.getCloudEntityTypeForProfileType(
                entityProfileDTO.getEntityType());
        if (!newCloudEntityType.isPresent()) {
            logger.warn("Skipping entity profile {} of type {}", builder.getDisplayName(),
                    entityProfileDTO.getEntityType().name());
            return;
        }

        builder.setEntityType(newCloudEntityType.get());
        newEntityBuildersById.put(profileId, builder);
        profileDTOsById.put(profileId, entityProfileDTO);
    }

    /**
     * Create cloud services based on the table
     * and then put into sharedEntities table for use by converters later.
     */
    private void createCloudServices() {
        conversionContext.getCloudServicesToCreate().forEach(cloudService -> {
            EntityDTO.Builder csBuilder = EntityDTO.newBuilder();
            csBuilder.setEntityType(EntityType.CLOUD_SERVICE);
            csBuilder.setId(cloudService.getId());
            csBuilder.setDisplayName(cloudService.getDisplayName());
            newEntityBuildersById.put(cloudService.getId(), csBuilder);
        });
    }

    /**
     * Get the original EntityDTO for the provided entity id. This DTO comes from probe and is
     * unmodified.
     *
     * @param entityId the id of the EntityDTO to get
     * @return original unmodified EntityDTO
     */
    public EntityDTO getRawEntityDTO(@Nonnull String entityId) {
        return rawEntityDTOsById.get(entityId);
    }

    /**
     * Get the new cloud entity builder for the provided entity id. This entity is in progress of
     * modification.
     *
     * @param entityId the id of the entity builder to get
     * @return builder for new cloud entity
     */
    public EntityDTO.Builder getNewEntityBuilder(@Nonnull String entityId) {
        return newEntityBuildersById.get(entityId);
    }

    @VisibleForTesting
    public Map<EntityType, List<EntityDTO.Builder>> getNewEntitiesGroupedByType() {
        return newEntityBuildersById.values().stream()
                .collect(Collectors.groupingBy(EntityDTO.Builder::getEntityType));
    }

    /**
     * Get the profile DTO for the provided profile id.
     *
     * @param profileId id of the profile dto to get
     * @return profile DTO
     */
    public EntityProfileDTO getProfileDTO(@Nonnull String profileId) {
        return profileDTOsById.get(profileId);
    }

    /**
     * Get the ids for all the storage tiers.
     *
     * @return set of all storage tier ids
     */
    public Set<String> getAllStorageTierIds() {
        return allStorageTierIds;
    }

    /**
     * Add the provided entity to be owned by business account. For AWS, master account is used
     * if any. For Azure, it used that single account discovered in probe, since it doesn't have
     * master account and every target just returns one business account.
     *
     * @param entityId id of the entity to be owned by business account
     */
    public void ownedByBusinessAccount(@Nonnull String entityId) {
        if (businessAccountToOwnEntities == null) {
            return;
        }
        if (!businessAccountToOwnEntities.getConsistsOfList().contains(entityId) &&
                !entityId.equals(businessAccountToOwnEntities.getId())) {
            businessAccountToOwnEntities.addConsistsOf(entityId);
        }
    }

    /**
     * Add the entity to be owned by the related cloud service.
     *
     * @param entityType EntityType of the entity to be owned by cloud service, which is used to
     * look up which cloud service to own
     * @param entityId id of the entity to be owned by cloud sevice
     */
    public void ownedByCloudService(@Nonnull EntityType entityType, @Nonnull String entityId) {
        Optional<CloudService> cloudService = conversionContext.getCloudServiceOwner(entityType);
        cloudService.ifPresent(cs -> {
            EntityDTO.Builder csBuilder = getNewEntityBuilder(cs.getId());
            if (!csBuilder.getConsistsOfList().contains(entityId)) {
                csBuilder.addConsistsOf(entityId);
            }
        });
    }

    /**
     * Split the key of access commodity and get the uuid from key. For cloud targets the DSPMAccess
     * key looks like "PhysicalMachine::aws::us-west-2::PM::us-west-2b", where the part after the
     * colons is the uuid.
     *
     * @param key original key
     * @return the uuid part of the key
     */
    @Nonnull
    public static String keyToUuid(@Nonnull String key) {
        return key.split("::", 2)[1];
    }

    /**
     * Get the region id based on the zone id.
     *
     * @param azId id of the availability zone
     * @return id of the related region
     */
    @Nonnull
    public String getRegionIdFromAzId(@Nonnull String azId) {
        return conversionContext.getRegionIdFromAzId(azId);
    }

    /**
     * Get the id for the storage tier.
     *
     * @param storageTier name of the storage tier
     * @return id of the storage tier
     */
    @Nonnull
    public String getStorageTierId(@Nonnull String storageTier) {
        return conversionContext.getStorageTierId(storageTier);
    }
}
