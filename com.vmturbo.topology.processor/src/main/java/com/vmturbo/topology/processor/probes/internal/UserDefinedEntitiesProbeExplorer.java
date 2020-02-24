package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.ENTITY_DEFINITION;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Explorer class for the 'UserDefinedEntities' probe.
 */
class UserDefinedEntitiesProbeExplorer {

    private static final Logger LOGGER = LogManager.getLogger();

    private final UserDefinedEntitiesProbeRetrieval retrieval;

    /**
     * Constructor.
     *
     * @param retrieval - a supplier fof groups and members.
     */
    UserDefinedEntitiesProbeExplorer(@Nonnull UserDefinedEntitiesProbeRetrieval retrieval) {
        this.retrieval = retrieval;
    }

    /**
     * Gets all groups with type 'ENTITY_DEFINITION'. And then retrieves their members.
     *
     * @return a map of groups.
     */
    Map<Grouping, Collection<TopologyEntityDTO>> getUserDefinedGroups() {
        final Map<Grouping, Collection<TopologyEntityDTO>> groupMap = Maps.newHashMap();
        try {
            final Collection<Grouping> groups = retrieval.getGroups(ENTITY_DEFINITION);
            LOGGER.info("Internal probe: found {} entity definitions", groups.size());
            for (Grouping group : groups) {
                final Collection<TopologyEntityDTO> members = retrieval.getMembers(group.getId());
                if (!members.isEmpty()) {
                    groupMap.put(group, members);
                } else {
                    LOGGER.warn("Internal probe: entity definition {} has no members, skipping it",
                            group.getDefinition().hasDisplayName()
                            ? group.getDefinition().getDisplayName()
                            : "null");
                }
            }
        } catch (Exception e) {
            LOGGER.error(e);
        }
        return groupMap;
    }
}
