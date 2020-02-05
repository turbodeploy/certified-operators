package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.common.builders.SDKConstants.ACCESS_COMMODITY_CAPACITY;
import static com.vmturbo.platform.common.builders.SDKConstants.ACCESS_COMMODITY_USED;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_TRANSACTION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE_SERVER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.SERVICE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.CommodityBuilders.ApplicationBought;
import com.vmturbo.platform.common.builders.CommodityBuilders.ApplicationSold;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.builders.GenericEntityBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse.Builder;

/**
 * A converter of entities for 'UserDefinedEntities' probe.
 */
class UserDefinedEntitiesProbeConverter {

    private static final Collection<EntityType> AVAILABLE_MEMBER_TYPES = Sets.newHashSet(
            BUSINESS_TRANSACTION, SERVICE, APPLICATION_COMPONENT,
            DATABASE_SERVER, VIRTUAL_MACHINE, CONTAINER
    );

    private static final Collection<EntityType> AVAILABLE_GROUP_TYPES = Sets.newHashSet(
            BUSINESS_APPLICATION, BUSINESS_TRANSACTION, SERVICE
    );

    /**
     * Makes a converting from groups into the response of the probe.
     *
     * @param groups - collection of groups.
     * @return {@link DiscoveryResponse} instance.
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    DiscoveryResponse convertToResponse(Map<Grouping, Collection<TopologyEntityDTO>> groups) {
        final Builder builder = DiscoveryResponse.newBuilder();
        final Set<EntityDTO> dtos = groups.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .flatMap(entry -> convertGroupAndMembers(entry.getKey(), entry.getValue()).stream())
                .collect(Collectors.toSet());
        builder.addAllEntityDTO(dtos);
        return builder.build();
    }

    @Nonnull
    private EntityType getGroupEntityType(@Nonnull Grouping grouping) {
        return grouping.getDefinition().getEntityDefinitionData().getDefinedEntityType();
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private ApplicationBought createApplicationBought(String providerId, String consumerId) {
        return CommodityBuilders.application()
                .from(providerId)
                .key(getApplicationCommodityKey(providerId, consumerId))
                .used(ACCESS_COMMODITY_USED);
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private ApplicationSold createApplicationSold(String providerId, String consumerId) {
        return CommodityBuilders.application()
                .sold()
                .key(getApplicationCommodityKey(providerId, consumerId))
                .capacity(ACCESS_COMMODITY_CAPACITY);
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private String getApplicationCommodityKey(String providerId, String consumerId) {
        return String.format("ApplicationCommodity::%s::%s", providerId, consumerId);
    }

    @Nonnull
    private String getMemberId(@Nonnull TopologyEntityDTO memberEntity) {
        return String.valueOf(memberEntity.getOid());
    }

    private boolean isSupportedMemberType(@Nonnull EntityType type) {
        return AVAILABLE_MEMBER_TYPES.contains(type);
    }

    private boolean isSupportedGroupType(@Nonnull EntityType type) {
        return AVAILABLE_GROUP_TYPES.contains(type);
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private Set<EntityDTO> convertGroupAndMembers(Grouping group, Collection<TopologyEntityDTO> members) {
        final Set<String> memberIds = members.stream().map(this::getMemberId).collect(Collectors.toSet());
        final EntityDTO entityDtoFromGroup = convertGroup(group, memberIds);
        if (entityDtoFromGroup != null) {
            final Set<EntityDTO> convertedMembers = members.stream()
                    .map(member -> convertMember(member, entityDtoFromGroup.getId()))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            if (!convertedMembers.isEmpty()) {
                convertedMembers.add(entityDtoFromGroup);
            }
            return convertedMembers;
        }
        return Sets.newHashSet();
    }

    @Nullable
    @ParametersAreNonnullByDefault
    private EntityDTO convertMember(TopologyEntityDTO memberEntity, String groupId) {
        final String memberId = getMemberId(memberEntity);
        final String displayName = memberEntity.getDisplayName();
        final String name = StringUtils.isNotEmpty(displayName) ? displayName : String.format("%s:%s", groupId, memberId);
        final EntityType type = EntityType.forNumber(memberEntity.getEntityType());
        if (isSupportedMemberType(type)) {
            return EntityBuilders.entity(memberId)
                    .entityType(type)
                    .selling(createApplicationSold(memberId, groupId))
                    .proxy(true)
                    .displayName(name)
                    .build();
        }
        return null;
    }

    @Nullable
    @ParametersAreNonnullByDefault
    private EntityDTO convertGroup(Grouping group, Set<String> memberIds) {
        final String groupId = String.valueOf(group.getId());
        final String originGroupName = group.getDefinition().getDisplayName();
        final String name = StringUtils.isNotEmpty(originGroupName) ? originGroupName : groupId;
        final Set<ApplicationBought> applicationBought = memberIds.stream()
                .map(memberId -> createApplicationBought(memberId, groupId))
                .collect(Collectors.toSet());
        final EntityType type = getGroupEntityType(group);
        if (isSupportedGroupType(type)) {
            final GenericEntityBuilder builder = EntityBuilders.entity(groupId)
                    .entityType(type)
                    .displayName(name);
            applicationBought.forEach(builder::buying);
            return builder.build();
        }
        return null;
    }

}
