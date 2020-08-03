package com.vmturbo.group.stitching;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Class to hold stitching results.
 */
public class StitchingResult {
    private final Collection<StitchingGroup> groupsToAddOrUpdate;
    private final Set<Long> groupsToDelete;

    /**
     * Constructs stitching results.
     *
     * @param groupsToAddOrUpdate groups to add or update
     * @param groupsToDelete groups to remove.
     */
    public StitchingResult(Collection<StitchingGroup> groupsToAddOrUpdate,
            Set<Long> groupsToDelete) {
        this.groupsToAddOrUpdate = Collections.unmodifiableCollection(groupsToAddOrUpdate);
        this.groupsToDelete = Collections.unmodifiableSet(groupsToDelete);
    }

    public Collection<StitchingGroup> getGroupsToAddOrUpdate() {
        return groupsToAddOrUpdate;
    }

    public Set<Long> getGroupsToDelete() {
        return groupsToDelete;
    }
}
