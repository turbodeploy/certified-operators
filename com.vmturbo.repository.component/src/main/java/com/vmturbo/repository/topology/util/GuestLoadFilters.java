package com.vmturbo.repository.topology.util;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;

/**
 * Contains methods for filtering GuestLoad applications.
 */
public final class GuestLoadFilters {
    @VisibleForTesting
    static final String APPLICATION_TYPE_PATH = "common_dto.EntityDTO.ApplicationData.type";

    /**
     * This is to prevent instantiation of this utility class.
     */
    private GuestLoadFilters() {
    }

    /**
     * Checks {@link TopologyEntityDTO} that it's a Guest Load application.
     *
     * @param entityDTO topology entity dto
     * @return Returns {@code true} if it's a Guest Load application.
     */
    public static boolean isGuestLoad(@Nonnull TopologyEntityDTO entityDTO) {
        return isApplicationType(entityDTO.getEntityType())
                && isGuestLoadType(entityDTO.getEntityPropertyMapMap());
    }

    /**
     * Checks {@link RepoGraphEntity} that it's not a Guest Load application.
     *
     * @param entity topology entity dto
     * @return Returns {@code true} if it's not a Guest Load application.
     */
    public static boolean isNotGuestLoad(@Nonnull TopologyEntityDTO entity) {
        return !isGuestLoad(entity);
    }

    /**
     * Checks an entity type that it's one of the Guest Load application types.
     *
     * @param type entity type
     * @return Returns {@code true} if this is one of the Guest Load application types.
     */
    private static boolean isApplicationType(int type) {
        return type == EntityType.APPLICATION_VALUE
                || type == EntityType.APPLICATION_COMPONENT_VALUE;
    }

    /**
     * Checks Application Type on Guest Load.
     *
     * @param properties application properties
     * @return Returns {@code true} if this is a Guest Load application type.
     */
    private static boolean isGuestLoadType(Map<String, String> properties) {
        return SupplyChainConstants.GUEST_LOAD.equals(properties.get(APPLICATION_TYPE_PATH));
    }
}
