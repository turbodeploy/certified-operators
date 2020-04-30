package com.vmturbo.history.db.queries;

import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_BY_MONTH;

import com.google.common.collect.Lists;

import org.jooq.ResultQuery;
import org.jooq.Table;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.history.db.QueryTestBase;

/**
 * Test class for {@link EntityCommoditiesMaxValuesQuery} query builder.
 */
public class EntityCommoditiesMaxValuesQueryTest extends QueryTestBase {

    /**
     * Test setup.
     */
    @Before
    public void before() {
        setupJooq();
    }

    /**
     * Test that query is properly constructed for VM stats.
     */
    @Test
    public void testVmStatsQuery() {
        final ResultQuery<?> query = new EntityCommoditiesMaxValuesQuery(VM_STATS_BY_MONTH, Lists.newArrayList("vCPU"), 90).getQuery();
        getQueryChecker(VM_STATS_BY_MONTH).check(query);
    }

    /**
     * Test that query is properly constructed for PM stats table.
     *
     * <p>Just so that we're checking two different tables - nothing particularly special about
     * VM and PM.</p>
     */
    @Test
    public void testPmStatsQuery() {
        final ResultQuery<?> query = new EntityCommoditiesMaxValuesQuery(PM_STATS_BY_MONTH, Lists.newArrayList("CPU"), 90).getQuery();
        getQueryChecker(PM_STATS_BY_MONTH).check(query);
    }

    private QueryChecker getQueryChecker(Table<?> table) {
        final String tableName = table.getName();
        return new QueryChecker()
                .withDistinct(false)
                .withSelectFields(
                        tableName + ".uuid",
                        tableName + ".property_type",
                        tableName + ".commodity_key",
                        "max\\(" + tableName + ".max_value\\)")
                .withTables(tableName)
                .withConditions(
                        tableName + ".snapshot_time >= TIMESTAMP '.*'",
                        tableName + ".property_type IN (.*)",
                        tableName + ".property_subtype = 'used'",
                        tableName + ".relation = 0")
                .withGroupByFields(
                        tableName + ".uuid",
                        tableName + ".property_type",
                        tableName + ".commodity_key")
                .withLimit(0);
    }
}
