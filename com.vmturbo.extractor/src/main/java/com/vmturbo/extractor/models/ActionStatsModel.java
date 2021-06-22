package com.vmturbo.extractor.models;

import java.sql.Timestamp;

import com.vmturbo.extractor.schema.Tables;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionMode;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;

/**
 * Definitions for action stats-related tables and columns for on-prem reporting.
 */
public class ActionStatsModel {

    /**
     * Private default constructor to prevent instantiation.
     */
    private ActionStatsModel() {}

    /**
     * The PENDING_ACTION_STATS table.
     */
    public static class PendingActionStats {

        /**
         * Private default constructor to prevent instantiation.
         */
        private PendingActionStats() {}

        /**
         * The TIME column.
         */
        public static final Column<Timestamp> TIME =
                Column.timestampColumn(Tables.PENDING_ACTION_STATS.TIME);

        /**
         * The SCOPE_OID column: a reference to a scope, which can be an entity or group. "0" for
         * global environment.
         */
        public static final Column<Long> SCOPE_OID =
                Column.longColumn(Tables.PENDING_ACTION_STATS.SCOPE_OID);

        /**
         * The ENTITY_TYPE column: the type of entities to which the stats belong within the scope.
         */
        public static final Column<EntityType> ENTITY_TYPE =
                new Column<>(Tables.PENDING_ACTION_STATS.ENTITY_TYPE, ColType.ENTITY_TYPE);

        /**
         * The ENVIRONMENT_TYPE column: the environment type of entities to which the stats belong
         * within the scope.
         */
        public static final Column<EnvironmentType> ENVIRONMENT_TYPE =
                new Column<>(Tables.PENDING_ACTION_STATS.ENVIRONMENT_TYPE, ColType.ENVIRONMENT_TYPE);

        /**
         * The ACTION_GROUP column: references the action_group table.
         */
        public static final Column<Integer> ACTION_GROUP =
                Column.intColumn(Tables.PENDING_ACTION_STATS.ACTION_GROUP);

        /**
         * The PRIOR_ACTION_COUNT column: total action count in the previous cycle/time range (at
         * the time of the last record).
         */
        public static final Column<Integer> PRIOR_ACTION_COUNT =
                Column.intColumn(Tables.PENDING_ACTION_STATS.PRIOR_ACTION_COUNT);

        /**
         * The CLEARED_ACTION_COUNT column: number of actions count cleared during this cycle (in
         * the time between the last record's time and this record's time).
         */
        public static final Column<Integer> CLEARED_ACTION_COUNT =
                Column.intColumn(Tables.PENDING_ACTION_STATS.CLEARED_ACTION_COUNT);

        /**
         * The NEW_ACTION_COUNT column: number of new action counts generated during this cycle.
         */
        public static final Column<Integer> NEW_ACTION_COUNT =
                Column.intColumn(Tables.PENDING_ACTION_STATS.NEW_ACTION_COUNT);

        /**
         * The INVOLVED_ENTITY_COUNT column: number of entities involved in the actions.
         */
        public static final Column<Integer> INVOLVED_ENTITY_COUNT =
                Column.intColumn(Tables.PENDING_ACTION_STATS.INVOLVED_ENTITY_COUNT);

        /**
         * The SAVINGS column: total savings for the actions.
         */
        public static final Column<Float> SAVINGS =
                Column.floatColumn(Tables.PENDING_ACTION_STATS.SAVINGS);

        /**
         * The INVESTMENTS column: total investments for the actions.
         */
        public static final Column<Float> INVESTMENTS =
                Column.floatColumn(Tables.PENDING_ACTION_STATS.INVESTMENTS);

        /**
         * The PENDING_ACTION_STATS table.
         */
        public static final Table TABLE = Table.named(Tables.PENDING_ACTION_STATS.getName())
                .withColumns(TIME, SCOPE_OID, ENTITY_TYPE, ENVIRONMENT_TYPE, ACTION_GROUP,
                        PRIOR_ACTION_COUNT, CLEARED_ACTION_COUNT, NEW_ACTION_COUNT,
                        INVOLVED_ENTITY_COUNT, SAVINGS, INVESTMENTS)
                .build();
    }

    /**
     * The ACTION_GROUP table.
     */
    public static class ActionGroup {

        /**
         * Private default constructor to prevent instantiation.
         */
        private ActionGroup() {}

        /**
         * The ID column: auto-incrementing database column to be used as primary key.
         *
         * <p>This is also used as a foreign key in the `PendingActionStats` table for joins.</p>
         */
        public static final Column<Integer> ID = Column.intColumn(Tables.ACTION_GROUP.ID);

        /**
         * The TYPE column: the type of action to which the stats belong within the scope.
         */
        public static final Column<ActionType> TYPE =
                new Column<>(Tables.ACTION_GROUP.TYPE, ColType.ACTION_TYPE);

        /**
         * The CATEGORY column: the category of action to which the stats belong within the scope.
         */
        public static final Column<ActionCategory> CATEGORY =
                new Column<>(Tables.ACTION_GROUP.CATEGORY, ColType.ACTION_CATEGORY);

        /**
         * The STATE column: the state of action to which the stats belong within the scope.
         */
        public static final Column<ActionState> STATE =
                new Column<>(Tables.ACTION_GROUP.STATE, ColType.ACTION_STATE);

        /**
         * The MODE column: the mode of action to which the stats belong within the scope.
         */
        public static final Column<ActionMode> MODE =
                new Column<>(Tables.ACTION_GROUP.MODE, ColType.ACTION_MODE);

        /**
         * The RISKS column: the risks that the action addresses (e.g. overutilized CPU).
         */
        public static final Column<String[]> RISKS = Column.stringArrayColumn(Tables.ACTION_GROUP.RISKS);

        /**
         * The ACTION_GROUP table.
         */
        public static final Table TABLE = Table.named(Tables.ACTION_GROUP.getName())
                .withColumns(ID, TYPE, CATEGORY, STATE, MODE, RISKS)
                .build();

    }
}
