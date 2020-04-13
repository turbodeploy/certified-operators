package com.vmturbo.topology.processor.targets;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.sdk.common.EntityPropertyName;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.targets.GroupScopeResolver.GroupScopedEntity;

/**
 * A class to provide convenience methods for extracting the properties needed by scoped probes
 * from members of the group that provides each probe's scope.
 */
public class GroupScopePropertyExtractor {
    private static final Logger logger = LogManager.getLogger();

    private static final String IP_ADDRESS_SEPARATOR = ",";
    // the static string used by VC probe as a prefix when generating entity ids
    private static final String VC_VSTORAGE_UUID_PREFIX = "_wK4GWWTbEd-Ea97W1fNhs6";
    private static final String BACKSLASH = "\\";

    /**
     * A map from each property that might appear in the group scope to a
     * {@link EntityPropertyExtractor} that can extract that property from an {@link Entity}
     */
    private final static Map<EntityPropertyName, EntityPropertyExtractor> extractorMap =
            ImmutableMap.<EntityPropertyName, EntityPropertyExtractor>builder()
                    .put(EntityPropertyName.DISPLAY_NAME, groupScopedEntity -> {
                            final TopologyEntityDTO topologyEntityDTO =
                                    groupScopedEntity.getTopologyEntityDTO();
                            return topologyEntityDTO.hasDisplayName() ?
                                    Optional.of(topologyEntityDTO.getDisplayName())
                                    : Optional.empty();

                    })
                    .put(EntityPropertyName.IP_ADDRESS, groupScopedEntity -> {
                            final TopologyEntityDTO topologyEntityDTO =
                                    groupScopedEntity.getTopologyEntityDTO();
                            if (topologyEntityDTO.hasTypeSpecificInfo() &&
                                    topologyEntityDTO.getTypeSpecificInfo()
                                            .hasVirtualMachine()) {
                                String combinedIps = topologyEntityDTO.getTypeSpecificInfo()
                                        .getVirtualMachine()
                                        .getIpAddressesList().stream()
                                        .map(IpAddress::getIpAddress)
                                        .filter(Strings::isNotBlank)
                                        .collect(Collectors.joining(IP_ADDRESS_SEPARATOR));
                                if (combinedIps.isEmpty()) {
                                    return Optional.empty();
                                }
                                return Optional.of(combinedIps);
                            } else {
                                return Optional.empty();
                            }
                    })
                    .put(EntityPropertyName.STATE, groupScopedEntity -> {
                            final TopologyEntityDTO topologyEntityDTO =
                                    groupScopedEntity.getTopologyEntityDTO();
                            return topologyEntityDTO.hasEntityState() ?
                                    Optional.of(topologyEntityDTO.getEntityState().name())
                                    : Optional.empty();
                    })
                    .put(EntityPropertyName.UUID, groupScopedEntity -> {
                            return Optional.of(
                                    String.valueOf(groupScopedEntity.getTopologyEntityDTO()
                                            .getOid()));
                    })
                    .put(EntityPropertyName.GUEST_LOAD_UUID, groupScopedEntity -> {
                            return groupScopedEntity.getGuestLoadEntityOid();
                    })
                    .put(EntityPropertyName.MEM_BALLOONING,
                            new CommodityCapacityExtractor(CommodityType.BALLOONING))
                    .put(EntityPropertyName.VCPU_CAPACITY,
                            new CommodityCapacityExtractor(CommodityType.VCPU))
                    .put(EntityPropertyName.VMEM_CAPACITY,
                            new CommodityCapacityExtractor(CommodityType.VMEM))
                    .put(EntityPropertyName.VSTORAGE_KEY_PREFIX, groupScopedEntity -> {
                        // construct the vstorage key prefix by combining the vc vstorage prefix,
                        // target address and the vm local name, for example:
                        //     _wK4GWWTbEd-Ea97W1fNhs6\vsphere-dc17.eng.vmturbo.com\vm-185
                        // this will be sent to application probe, which will append a drive name:
                        //     _wK4GWWTbEd-Ea97W1fNhs6\vsphere-dc17.eng.vmturbo.com\vm-185_C:\
                        // then prepend "VirtualMachine::", hash and generate the same key as that
                        // in VC like:
                        //     VirtualMachine::c49e5dd7cb5863297b1196e1670f92cbb5f4c163
                        final Optional<String> targetAddress = groupScopedEntity.getTargetAddress();
                        final Optional<String> localName = groupScopedEntity.getLocalName();
                        if (!targetAddress.isPresent()) {
                            logger.error("Target address not found for entity: {}",
                                groupScopedEntity.getTopologyEntityDTO().getOid());
                            return Optional.empty();
                        }
                        if (!localName.isPresent()) {
                            logger.error("LocalName not found for entity: {}",
                                groupScopedEntity.getTopologyEntityDTO().getOid());
                            return Optional.empty();
                        }
                        return Optional.of(VC_VSTORAGE_UUID_PREFIX + BACKSLASH +
                            targetAddress.get() + BACKSLASH + localName.get());
                    })
                .put(EntityPropertyName.OFFER_ID, groupScopedEntity -> {
                    final TopologyEntityDTO topologyEntityDTO =
                        groupScopedEntity.getTopologyEntityDTO();
                    if (topologyEntityDTO.hasTypeSpecificInfo() &&
                        topologyEntityDTO.getTypeSpecificInfo().hasBusinessAccount()) {
                        return topologyEntityDTO.getTypeSpecificInfo()
                            .getBusinessAccount().getPricingIdentifiersList().stream()
                            .filter(pricingId -> pricingId.getIdentifierName() ==
                                PricingIdentifierName.OFFER_ID)
                            .map(PricingIdentifier::getIdentifierValue)
                            .findFirst();
                    }
                    return Optional.empty();
                })
                .put(EntityPropertyName.ENROLLMENT_NUMBER, groupScopedEntity -> {
                    final TopologyEntityDTO topologyEntityDTO =
                        groupScopedEntity.getTopologyEntityDTO();
                    if (topologyEntityDTO.hasTypeSpecificInfo() &&
                        topologyEntityDTO.getTypeSpecificInfo().hasBusinessAccount()) {
                        return topologyEntityDTO.getTypeSpecificInfo()
                            .getBusinessAccount().getPricingIdentifiersList().stream()
                            .filter(pricingId -> pricingId.getIdentifierName() ==
                                PricingIdentifierName.ENROLLMENT_NUMBER)
                            .map(PricingIdentifier::getIdentifierValue)
                            .findFirst();
                    }
                    return Optional.empty();
                })
                    .build();

    /**
     * Extract and return the named entity property from a GroupScopedEntity.
     * @param entityProperty {@link EntityPropertyName} giving the property to extract.
     * @param groupScopedEntity {@link GroupScopedEntity} representing the object we want to extract
     *                                                  the property from.
     * @return {@link Optional} string with the value of the property or Optional.empty if the
     * property does not exist for the entity.
     */
    public static Optional<String> extractEntityProperty(
            @Nonnull final EntityPropertyName entityProperty,
            @Nonnull final GroupScopedEntity groupScopedEntity) {
        EntityPropertyExtractor extractor =
                extractorMap.get(Objects.requireNonNull(entityProperty));
        if (extractor == null) {
            logger.error("No extractor found for entity property {} when creating group scope.",
                    entityProperty.name());
            return Optional.empty();
        }
        return extractor.getValue(Objects.requireNonNull(groupScopedEntity));
    }

    @FunctionalInterface
    private interface EntityPropertyExtractor {
        Optional<String> getValue(GroupScopedEntity groupScopedEntity);
    }

    /**
     * Class to return the capacity of the first sold commodity that matches the CommodityType
     * passed into the constructor.
     */
    private static class CommodityCapacityExtractor implements EntityPropertyExtractor {
        private final CommodityType commType;

        public CommodityCapacityExtractor(@Nonnull CommodityType commType) {
            this.commType = Objects.requireNonNull(commType);
        }

        @Override
        public Optional<String> getValue(final GroupScopedEntity groupScopedEntity) {
            return groupScopedEntity.getTopologyEntityDTO().getCommoditySoldListList().stream()
                    .filter(comm -> commType.getNumber()
                            == comm.getCommodityType().getType())
                    .filter(CommoditySoldDTO::hasCapacity)
                    .map(CommoditySoldDTO::getCapacity)
                    .map(String::valueOf)
                    .findFirst();
        }
    }
}
