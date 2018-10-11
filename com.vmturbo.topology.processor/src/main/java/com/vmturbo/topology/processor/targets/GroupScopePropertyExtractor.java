package com.vmturbo.topology.processor.targets;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.EntityPropertyName;
import com.vmturbo.topology.processor.entity.Entity;

/**
 * A class to provide convenience methods for extracting the properties needed by scoped probes
 * from members of the group that provides each probe's scope.
 */
public class GroupScopePropertyExtractor {
    private static final Logger logger = LogManager.getLogger();

    public static final char VSTORAGE_PREFIX_SEPARATOR = '_';
    public static final String IP_ADDRESS_SEPARATOR = ",";

    /**
     * A map from each property that might appear in the group scope to a
     * {@link EntityPropertyExtractor} that can extract that property from an {@link Entity}
     */
    // TODO write extractor to handle EntityPropertyName.GUEST_LOAD_UUID
    private final static Map<EntityPropertyName, EntityPropertyExtractor> extractorMap =
            ImmutableMap.<EntityPropertyName, EntityPropertyExtractor>builder()
                    .put(EntityPropertyName.DISPLAY_NAME,
                            new EntityPropertyExtractor() {
                                @Override
                                public Optional<String> getValue(final TopologyEntityDTO topologyEntityDTO) {
                                    return topologyEntityDTO.hasDisplayName() ?
                                            Optional.of(topologyEntityDTO.getDisplayName())
                                            : Optional.empty();
                                }
                            })
                    .put(EntityPropertyName.IP_ADDRESS,
                            new EntityPropertyExtractor()
                        {
                                @Override
                                public Optional<String> getValue(
                                        final TopologyEntityDTO topologyEntityDTO) {
                                    if (topologyEntityDTO.hasTypeSpecificInfo() &&
                                            topologyEntityDTO.getTypeSpecificInfo()
                                                    .hasVirtualMachine()) {
                                        String combinedIps = topologyEntityDTO.getTypeSpecificInfo().getVirtualMachine()
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
                                }
                            })
                    .put(EntityPropertyName.STATE, new EntityPropertyExtractor() {
                        @Override
                        public Optional<String> getValue(final TopologyEntityDTO topologyEntityDTO) {
                            return topologyEntityDTO.hasEntityState() ?
                                    Optional.of(topologyEntityDTO.getEntityState().name())
                                    : Optional.empty();
                        }
                    })
                    .put(EntityPropertyName.UUID, new EntityPropertyExtractor() {
                        @Override
                        public Optional<String> getValue(final TopologyEntityDTO topologyEntityDTO) {
                            return Optional.of(Long.toString(topologyEntityDTO.getOid()));
                        }
                    })
                    .put(EntityPropertyName.MEM_BALLOONING,
                            new CommodityCapacityExtractor(CommodityType.BALLOONING))
                    .put(EntityPropertyName.VCPU_CAPACITY,
                            new CommodityCapacityExtractor(CommodityType.VCPU))
                    .put(EntityPropertyName.VMEM_CAPACITY,
                            new CommodityCapacityExtractor(CommodityType.VMEM))
                    .put(EntityPropertyName.VSTORAGE_KEY_PREFIX,
                            new EntityPropertyExtractor() {
                                @Override
                                public Optional<String> getValue(final TopologyEntityDTO entity) {
                                    // If the VStorage key prefixes are all identical, return the
                                    // prefix; otherwise, return Optional.empty
                                    Set<String> prefixes = entity.getCommoditySoldListList()
                                            .stream()
                                            .filter(comm -> CommodityType.VSTORAGE.getNumber()
                                                    == comm.getCommodityType().getType())
                                            .map(CommoditySoldDTO::getCommodityType)
                                            .map(TopologyDTO.CommodityType::getKey)
                                            .map(key -> {
                                                int index =
                                                        key.lastIndexOf(VSTORAGE_PREFIX_SEPARATOR);
                                                return key.substring(0, index + 1);
                                            })
                                            .collect(Collectors.toSet());
                                    if (prefixes.size() == 1) {
                                        return Optional.of(prefixes.iterator().next());
                                    }
                                    return Optional.empty();
                                }
                            }
                    )
                    .build();

    /**
     * Extract and return the named entity property from a TopologyEntityDTO.
     * @param entityProperty {@link EntityPropertyName} giving the property to extract.
     * @param topoEntity {@link TopologyEntityDTO} representing the object we want to extract the
     *                                            property from.
     * @return {@link Optional} string with the value of the property or Optional.empty if the
     * property does not exist for the entity.
     */
    public static Optional<String> extractEntityProperty(
            @Nonnull final EntityPropertyName entityProperty,
            @Nonnull final TopologyEntityDTO topoEntity) {
        EntityPropertyExtractor extractor =
                extractorMap.get(Objects.requireNonNull(entityProperty));
        if (extractor == null) {
            logger.error("No extractor found for entity property {} when creating group scope.",
                    entityProperty.name());
            return Optional.empty();
        }
        return extractor.getValue(Objects.requireNonNull(topoEntity));
    }

    private interface EntityPropertyExtractor {
        Optional<String> getValue(TopologyEntityDTO topologyEntityDTO);
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
        public Optional<String> getValue(final TopologyEntityDTO topologyEntityDTO) {
            return topologyEntityDTO.getCommoditySoldListList().stream()
                    .filter(comm -> commType.getNumber()
                            == comm.getCommodityType().getType())
                    .filter(CommoditySoldDTO::hasCapacity)
                    .map(CommoditySoldDTO::getCapacity)
                    .map(String::valueOf)
                    .findFirst();
        }
    }
}
