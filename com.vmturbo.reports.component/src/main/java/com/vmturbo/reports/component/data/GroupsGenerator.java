package com.vmturbo.reports.component.data;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.sql.utils.DbException;

/**
 * Generate groups and their members relationships.
 * E.g. VM groups, and PM groups
 */
abstract public class GroupsGenerator {
    protected final GroupGeneratorDelegate generatorDelegate;
    public GroupsGenerator(@Nonnull final GroupGeneratorDelegate groupGeneratorDelegate) {
        this.generatorDelegate = groupGeneratorDelegate;
    }

    protected void insertVMGroups(final @Nonnull ReportsDataContext context) throws DbException {
        generatorDelegate.insertVMClusterRelationships(context);
    }

    protected Optional<String> insertVMGroup(final @Nonnull ReportsDataContext context,
                                     long groupId) throws DbException {
       return generatorDelegate.insertVMGroupRelationships(context, groupId);
    }

    protected Optional<String> insertPMGroup(final @Nonnull ReportsDataContext context,
                                             long groupId) throws DbException {
        return generatorDelegate.insertPMGroupRelationships(context, groupId);
    }

    // return void because it's hardcoded to always use "fake_vm_group" name
    protected void insertPMGroupAndVMRelationships(final @Nonnull ReportsDataContext context,
                                             long groupId) throws DbException {
        generatorDelegate.insertPMGroupAndVMRelationships(context, groupId);

    }

    protected void insertPMGroups(final @Nonnull ReportsDataContext context) throws DbException {
        generatorDelegate.insertPMClusterRelationships(context);
    }

    protected void insertStorageGroups(final @Nonnull ReportsDataContext context) throws DbException {
        generatorDelegate.insertStorageClusterRelationships(context);
    }

    protected Optional<String> insertPMVMsRelationships(@Nonnull final ReportsDataContext context,
                                                        final Long pmOid) throws DbException {
        return generatorDelegate.insertPMVMsRelationships(context, pmOid);
    }
}
