package com.vmturbo.search.metadata.utils;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.Type;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Utils for creating fields defined in {@link SearchMetadataMapping}.
 */
public class MetadataMappingUtils {

    /**
     * Private constructor for metadata.
     */
    private MetadataMappingUtils() {}

    /**
     * Get origin of the group.
     *
     * @param grouping {@link Grouping}
     * @return optional origin of the group
     */
    public static Optional<Object> getOrigin(Grouping grouping) {
        switch (grouping.getOrigin().getCreationOriginCase()) {
            case USER:
                return Optional.of(Type.USER.name());
            case DISCOVERED:
                return Optional.of(Type.DISCOVERED.name());
            case SYSTEM:
                return Optional.of(Type.SYSTEM.name());
            default:
                return Optional.empty();
        }
    }

    /**
     * Get direct member types of the given group.
     *
     * @param groupDefinition group definition
     * @return optional list of member type strings
     */
    public static List<String> getDirectMemberTypes(@Nonnull GroupDefinition groupDefinition) {
        return GroupProtoUtil.getDirectMemberTypes(groupDefinition)
                .map(memberType -> {
                    switch (memberType.getTypeCase()) {
                        case ENTITY:
                            EntityType entityType = EntityType.forNumber(memberType.getEntity());
                            return entityType == null ? null : entityType.name();
                        case GROUP:
                            return memberType.getGroup().name();
                        default:
                            return null;
                    }
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Get indirect member types of the given group. This includes all direct + indirect member
     * types, for a group of clusters, this will be [cluster, host].
     *
     * @param grouping group
     * @return optional list of member type strings
     */
    public static List<String> getIndirectMemberTypes(@Nonnull Grouping grouping) {
        return grouping.getExpectedTypesList().stream()
                .map(MemberType::getEntity)
                .map(EntityDTO.EntityType::forNumber)
                .filter(Objects::nonNull)
                .map(Enum::name)
                .collect(Collectors.toList());
    }
}
