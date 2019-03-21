package com.vmturbo.reports.component.data.vm;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.ReportsDataContext;
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

    protected void insertVMGroups(ReportsDataContext context) throws DbException {
        generatorDelegate.insertVMGroupRelationships(context);
    }

    protected void insertPMGroups(ReportsDataContext context) throws DbException {
        generatorDelegate.insertPMGroupRelationships(context);
    }
}
