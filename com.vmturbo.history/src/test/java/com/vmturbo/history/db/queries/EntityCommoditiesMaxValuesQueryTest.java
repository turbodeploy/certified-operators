package com.vmturbo.history.db.queries;

import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_BY_MONTH;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
        final ResultQuery<?> query = new EntityCommoditiesMaxValuesQuery(VM_STATS_BY_MONTH, Lists.newArrayList("vCPU"), 90, dsl).getQuery();
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
        final ResultQuery<?> query = new EntityCommoditiesMaxValuesQuery(PM_STATS_BY_MONTH,
                Lists.newArrayList("CPU"), 90, dsl).getQuery();
        getQueryChecker(PM_STATS_BY_MONTH).check(query);
    }

    /**
     * Test that query is properly constructed.
     */
    @Test
    public void testWithUuidListNoTempTable() {
        List<Long> uuids = Arrays.asList(1L, 2L, 3L);
        final ResultQuery<?> query = new EntityCommoditiesMaxValuesQuery(VM_STATS_BY_MONTH,
                Lists.newArrayList("CPU"), 90,
                true, uuids, dsl).getQuery();
        getQueryChecker(VM_STATS_BY_MONTH, true, uuids).check(query);
    }

    private QueryChecker getQueryChecker(Table<?> table) {
        return getQueryChecker(table, false, Collections.emptyList());
    }

    private QueryChecker getQueryChecker(Table<?> table, boolean boughtCommodities,
            List<Long> uuids) {
        final String tableName = table.getName();
        QueryChecker checker = new QueryChecker()
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
                        tableName + ".property_subtype = 'used'")
                .withGroupByFields(
                        tableName + ".uuid",
                        tableName + ".property_type",
                        tableName + ".commodity_key")
                .withLimit(0);
        if (boughtCommodities) {
            checker = checker.withSelectFields(tableName + ".producer_uuid")
                    .withConditions(tableName + ".relation = 1")
                    .withGroupByFields(tableName + ".producer_uuid");
        } else {
            checker = checker.withConditions(tableName + ".relation = 0");
        }
        if (!uuids.isEmpty()) {
            checker = checker.withConditions(
                    String.format("%s.uuid\\s+IN\\s+\\(%s\\s+\\)", tableName,
                            uuids.stream().map(uuid -> "\\s*'" + uuid + "'")
                                    .collect(Collectors.joining(", "))));
        }
        return checker;
    }
}
