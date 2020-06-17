package com.vmturbo.mediation.udt;

import static com.vmturbo.mediation.udt.ConverterUtils.createApplicationBought;
import static com.vmturbo.mediation.udt.ConverterUtils.createApplicationSold;
import static com.vmturbo.mediation.udt.UdtProbe.UDT_PROBE_TAG;
import static com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse.newBuilder;
import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.VENDOR;
import static com.vmturbo.platform.sdk.common.util.SDKUtil.VENDOR_ID;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.builders.GenericEntityBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Class responsible for converting UDT entities to {@link DiscoveryResponse}.
 */
class UdtProbeConverter {

    private final Set<UdtEntity> entities;

    /**
     * Constructor.
     *
     * @param entities - UDT entities to convert.
     */
    UdtProbeConverter(Set<UdtEntity> entities) {
        this.entities = entities;
    }

    /**
     * Creates {@link DiscoveryResponse} from UDT entities.
     * - first it converts all UDT entities (not children);
     * - then it converts children.
     *
     * @return DiscoveryResponse.
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    DiscoveryResponse createDiscoveryResponse() {
        Map<String, GenericEntityBuilder> entitiesDtoMap = Maps.newHashMap();
        // First create UDT entities
        entities.stream().map(this::createUdtDto).forEach(dto -> entitiesDtoMap.put(dto.getId(), dto));
        // Create UDT children (proxy objects)
        for (UdtEntity entity : entities) {
            for (UdtChildEntity child : entity.getChildren()) {
                String childId = String.valueOf(child.getOid());
                GenericEntityBuilder udtDto = entitiesDtoMap.get(entity.getDtoId());
                GenericEntityBuilder childDto = entitiesDtoMap
                        .getOrDefault(childId, createUdtChildDto(childId, child.getEntityType()));
                linkEntities(udtDto, childDto);
                entitiesDtoMap.put(childDto.getId(), childDto);
            }
        }
        Set<EntityDTO> entities = entitiesDtoMap.values().stream()
                .map(GenericEntityBuilder::build).collect(Collectors.toSet());
        return newBuilder().addAllEntityDTO(entities).build();
    }

    /**
     * Creates EntityDTO builder from UDT entity. It puts VENDOR and VENDOR_ID to
     * the builder`s properties.
     *
     * @param entity UDT entity.
     * @return EntityDTO builder.
     */
    @Nonnull
    @VisibleForTesting
    GenericEntityBuilder createUdtDto(@Nonnull UdtEntity entity) {
        return EntityBuilders.entity(entity.getDtoId())
                .entityType(entity.getEntityType())
                .displayName(entity.getName())
                .property(VENDOR, UDT_PROBE_TAG)
                .property(VENDOR_ID, entity.getId());
    }

    /**
     * Creates EntityDTO builder for a child of a UDT entity.
     *
     * @param id         - ID of child.
     * @param entityType - type of child.
     * @return EntityDTO builder.
     */
    @Nonnull
    @VisibleForTesting
    @ParametersAreNonnullByDefault
    GenericEntityBuilder createUdtChildDto(String id, EntityType entityType) {
        return EntityBuilders.entity(id)
                .entityType(entityType)
                .proxy(false);
    }

    /**
     * Links two entities using APPLICATION commodity.
     * - creates buy commodity for a UDT entity;
     * - creates sell commodity for child entity.
     *
     * @param udtEntity - UDT entity.
     * @param udtChild  - child entity.
     */
    @VisibleForTesting
    @ParametersAreNonnullByDefault
    void linkEntities(GenericEntityBuilder udtEntity, GenericEntityBuilder udtChild) {
        udtEntity.buying(createApplicationBought(udtChild.getId(), udtEntity.getId()));
        udtChild.selling(createApplicationSold(udtChild.getId(), udtEntity.getId()));
    }

}
