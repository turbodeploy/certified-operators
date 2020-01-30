package com.vmturbo.topology.processor.probes.internal;

import java.util.Collection;
import java.util.Collections;

import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;

/**
 * Explorer class for the 'UserDefinedEntities' probe.
 */
public class UserDefinedEntitiesProbeExplorer {

    /**
     * Makes a request to the Group component for getting user-defined groups.
     *
     * @return a collection of groups
     * @throws Exception in case of any request problems.
     */
    public Collection<GroupDTO> getUserDefinedGroups() throws Exception {
        // TODO: implement a request to the 'Group' component.
        return Collections.emptySet();
    }
}
