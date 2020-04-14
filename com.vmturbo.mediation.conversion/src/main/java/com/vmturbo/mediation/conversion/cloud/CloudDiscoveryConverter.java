package com.vmturbo.mediation.conversion.cloud;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.vmturbo.mediation.conversion.cloud.converter.DefaultConverter;
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

    private static final IEntityConverter defaultConverter = new DefaultConverter();

    private Map<String, EntityDTO> rawEntityDTOsById = new HashMap<>();

    protected Map<String, EntityDTO.Builder> newEntityBuildersById = new HashMap<>();

    private Map<String, EntityProfileDTO> profileDTOsById = new HashMap<>();

    private final DiscoveryResponse.Builder discoveryResponseBuilder;

    private final CloudProviderConversionContext conversionContext;

    /**
     * Suffix appended to the ephemeral volume id.
     */
    public static final String EPHEMERAL = "Ephemeral";

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
    @VisibleForTesting
    public void preProcess() {
        discoveryResponseBuilder.getEntityProfileList()
            .forEach(entityProfileDTO -> profileDTOsById.put(entityProfileDTO.getId(), entityProfileDTO));
        // initial loop through entities list, prepare for entity type specific converter later
        discoveryResponseBuilder.getEntityDTOList().forEach(this::preProcessEntityDTO);
    }

    /**
     * Pre process an EntityDTO to prepare for entity type specific conversion late, such as:
     * change the fake cloud entity type to the new "explicit" cloud entity type, create new
     * empty (no commodities) storage tiers, etc.
     *
     * @param entityDTO the EntityDTO to pre process
     */
    protected void preProcessEntityDTO(@Nonnull EntityDTO entityDTO) {
        EntityDTO.Builder entityBuilder = entityDTO.toBuilder();
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

    /**
     * Constructs the volume id for ephemeral volumes.
     * @param entityOid - the oid of the entity the instance store is on.
     * @param i - the volume indexNull region name fo
     * @param diskType - the disk type.
     * @return - volume id
     */
    public String createEphemeralVolumeId(final String entityOid,
                                          final int i, final String diskType) {
        final String volumePath =
                String.join( "_", ImmutableList.of(entityOid, diskType, (EPHEMERAL + i)));
        return volumePath;
    }
}
