package com.vmturbo.cost.component.reserved.instance.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderKeywordStyle;
import org.jooq.impl.DefaultDSLContext;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter.AccountFilterType;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class to test the ReservedInstanceBoughtFilter.
 */
public class ReservedInstanceBoughtFilterTest {

    private static final Long REGION_ID = 1L;
    private static final Long ZONE_ID = 2L;
    private static final String REGION_CLAUSE = "\"cost\".\"reserved_instance_spec\".\"region_id\" in (" + REGION_ID + ")";
    private static final String OR = "or";
    private static final String ZONE_CLAUSE = "\"cost\".\"reserved_instance_bought\".\"availability_zone_id\" in (" + ZONE_ID + ")";
    private static final String USED_DISCOVERED_ACCOUNT_CLAUSE =
            "\"cost\".\"reserved_instance_bought\".\"id\" in ("
            + " select \"cost\".\"entity_to_reserved_instance_mapping\".\"reserved_instance_id\""
            + " from \"cost\".\"reserved_instance_coverage_latest\""
            + " join \"cost\".\"entity_to_reserved_instance_mapping\""
            + " on \"cost\".\"reserved_instance_coverage_latest\".\"entity_id\" = \"cost\".\"entity_to_reserved_instance_mapping\".\"entity_id\""
            + " where \"cost\".\"reserved_instance_coverage_latest\".\"business_account_id\" in (10))";
    private static final String USED_UNDISCOVERED_ACCOUNT_CLAUSE =
            "\"cost\".\"reserved_instance_bought\".\"id\" in ("
            + " select \"cost\".\"account_to_reserved_instance_mapping\".\"reserved_instance_id\""
            + "from\"cost\".\"account_to_reserved_instance_mapping\""
            + " where \"cost\".\"account_to_reserved_instance_mapping\".\"business_account_oid\" in (10))";
    private static final String USED_ACCOUNT_CLAUSE =
            "(" + USED_DISCOVERED_ACCOUNT_CLAUSE + "or" + USED_UNDISCOVERED_ACCOUNT_CLAUSE + ")";
    private static final String PURCHASED_ACCOUNT_CLAUSE =
            "\"cost\".\"reserved_instance_bought\".\"business_account_id\" in (10)";

    private static final String USED_PURCHASED_ACCOUNT_CLAUSE =
            "(" + PURCHASED_ACCOUNT_CLAUSE + "or" + USED_DISCOVERED_ACCOUNT_CLAUSE
                    + "or" + USED_UNDISCOVERED_ACCOUNT_CLAUSE + ")";

    private static final String USED_REGION_CLAUSE =
            "\"cost\".\"reserved_instance_bought\".\"id\" in ("
                    + " select \"cost\".\"entity_to_reserved_instance_mapping\".\"reserved_instance_id\""
                    + " from \"cost\".\"reserved_instance_coverage_latest\""
                    + " join \"cost\".\"entity_to_reserved_instance_mapping\""
                    + " on \"cost\".\"reserved_instance_coverage_latest\".\"entity_id\" = \"cost\".\"entity_to_reserved_instance_mapping\".\"entity_id\""
                    + " where \"cost\".\"reserved_instance_coverage_latest\".\"region_id\" in (10))";
    private static final String USED_ZONE_CLAUSE =
            "\"cost\".\"reserved_instance_bought\".\"id\" in ("
                    + " select \"cost\".\"entity_to_reserved_instance_mapping\".\"reserved_instance_id\""
                    + " from \"cost\".\"reserved_instance_coverage_latest\""
                    + " join \"cost\".\"entity_to_reserved_instance_mapping\""
                    + " on \"cost\".\"reserved_instance_coverage_latest\".\"entity_id\" = \"cost\".\"entity_to_reserved_instance_mapping\".\"entity_id\""
                    + " where \"cost\".\"reserved_instance_coverage_latest\".\"availability_zone_id\" in (10))";


    /**
     * Tests if the conditions generated in the filter class. They should be treated as separated
     * conditions that will be AND together.
     */
    @Test
    public void testRegionAndAZConditions() {

        ReservedInstanceBoughtFilter testFilter =
                ReservedInstanceBoughtFilter.newBuilder()
                        .cloudScopeTuples(
                                ImmutableMap.of(EntityType.REGION, ImmutableSet.of(REGION_ID),
                                        EntityType.AVAILABILITY_ZONE, ImmutableSet.of(ZONE_ID)))
                        .build();
        final List<Condition> conditions = ImmutableList.copyOf(testFilter.generateConditions());
        final List<String> conditionStrings = conditions.stream()
                .map(Condition::toString)
                .collect(ImmutableList.toImmutableList());

        assertThat(conditions, hasSize(2));
        assertThat(conditionStrings.get(0), containsString(REGION_CLAUSE));
        assertThat(conditionStrings.get(0), containsString(OR));
        assertThat(conditionStrings.get(0), containsString(ZONE_CLAUSE));
    }

    /**
     * Tests the generate conditiosn for used by account filter type.
     */
    @Test
    public void testUsedAccountFilterTypeConditions() {

        final Connection conn = mock(Connection.class);
        DSLContext ctx = new DefaultDSLContext(conn, SQLDialect.MARIADB);
        ctx.settings().setRenderFormatted(true);
        ctx.settings().setRenderKeywordStyle(RenderKeywordStyle.UPPER);
        ReservedInstanceBoughtFilter testFilter =
                ReservedInstanceBoughtFilter.newBuilder()
                        .accountFilter(AccountFilter.newBuilder()
                                .setAccountFilterType(AccountFilterType.USED_BY)
                                .addAccountId(10).build())
                        .build();
        final List<Condition> conditions =
                ImmutableList.copyOf(testFilter.generateConditions(ctx));
        final List<String> conditionStrings = conditions.stream()
                .map(Condition::toString)
                .map(s -> s.replaceAll("\\s", ""))
                .collect(ImmutableList.toImmutableList());
        Assert.assertTrue(conditionStrings.contains(
                USED_ACCOUNT_CLAUSE.replaceAll("\\s", "")));
    }

    /**
     * Tests the generate condition for purhased and used by account filter type.
     */
    @Test
    public void testUsedAndPurchasedAccountFilterTypeCondition() {

        final Connection conn = mock(Connection.class);
        DSLContext ctx = new DefaultDSLContext(conn, SQLDialect.MARIADB);
        ctx.settings().setRenderFormatted(true);
        ctx.settings().setRenderKeywordStyle(RenderKeywordStyle.UPPER);
        ReservedInstanceBoughtFilter testFilter =
                ReservedInstanceBoughtFilter.newBuilder()
                        .accountFilter(AccountFilter.newBuilder()
                                .setAccountFilterType(AccountFilterType.USED_AND_PURCHASED_BY)
                                .addAccountId(10).build())
                        .build();
        final List<Condition> conditions =
                ImmutableList.copyOf(testFilter.generateConditions(ctx));
        final List<String> conditionStrings = conditions.stream()
                .map(Condition::toString)
                .map(s -> s.replaceAll("\\s", ""))
                .collect(ImmutableList.toImmutableList());
        Assert.assertTrue(conditionStrings.contains(USED_PURCHASED_ACCOUNT_CLAUSE
                .replaceAll("\\s", "")));

    }

    /**
     * Tests the condition for selecting used Ris given a region filter scope.
     */
    @Test
    public void testGenerateUsedByDiscoveredAccountsConditionRegion() {
        ReservedInstanceBoughtFilter testFilter =
                ReservedInstanceBoughtFilter.newBuilder()
                        .regionFilter(RegionFilter.newBuilder()
                                .addRegionId(10).build())
                        .build();
        testAndVerifyCondition(testFilter, USED_REGION_CLAUSE);
    }

    /**
     * Tests the condition for selecting used Ris given an account filter scope.
     */
    @Test
    public void testGenerateUsedByDiscoveredAccountsConditionAccount() {
        ReservedInstanceBoughtFilter testFilter =
                ReservedInstanceBoughtFilter.newBuilder()
                        .accountFilter(AccountFilter.newBuilder()
                                .addAccountId(10).build())
                        .build();
        testAndVerifyCondition(testFilter, USED_DISCOVERED_ACCOUNT_CLAUSE);
    }

    /**
     * Tests the condition for selecting used Ris given a zonal filter scope.
     */
    @Test
    public void testGenerateUsedByDiscoveredAccountsConditionZone() {
        ReservedInstanceBoughtFilter testFilter =
                ReservedInstanceBoughtFilter.newBuilder()
                        .availabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                                .addAvailabilityZoneId(10).build())
                        .build();
        testAndVerifyCondition(testFilter, USED_ZONE_CLAUSE);
    }

    /**
        * Tests the generate condition for no filters.
        */
    @Test
    public void testNoFilterTypeCondition() {

        final Connection conn = mock(Connection.class);
        DSLContext ctx = new DefaultDSLContext(conn, SQLDialect.MARIADB);
        ctx.settings().setRenderFormatted(true);
        ctx.settings().setRenderKeywordStyle(RenderKeywordStyle.UPPER);
        ReservedInstanceBoughtFilter testFilter =
                                                ReservedInstanceBoughtFilter.newBuilder()
                                                                .build();
        final List<Condition> conditions =
                                         ImmutableList.copyOf(testFilter.generateConditions(ctx));
        final List<String> conditionStrings = conditions.stream()
                        .map(Condition::toString)
                        .map(s -> s.replaceAll("\\s", ""))
                        .collect(ImmutableList.toImmutableList());

        // Asset that none of the filters is set, hence all global RIs should be retrieved.
        Assert.assertFalse(conditionStrings.contains(USED_PURCHASED_ACCOUNT_CLAUSE
                        .replaceAll("\\s", "")));
        Assert.assertFalse(conditionStrings.contains(USED_ACCOUNT_CLAUSE
                                                     .replaceAll("\\s", "")));
        Assert.assertFalse(conditionStrings.contains(USED_REGION_CLAUSE
                                                     .replaceAll("\\s", "")));
        Assert.assertFalse(conditionStrings.contains(USED_ZONE_CLAUSE
                                                     .replaceAll("\\s", "")));
        Assert.assertFalse(conditionStrings.contains(USED_UNDISCOVERED_ACCOUNT_CLAUSE
                                                     .replaceAll("\\s", "")));

    }

    private void testAndVerifyCondition(final ReservedInstanceBoughtFilter testFilter,
                                        final String expectedClause) {
        final Connection conn = mock(Connection.class);
        DSLContext ctx = new DefaultDSLContext(conn, SQLDialect.MARIADB);
        ctx.settings().setRenderFormatted(true);
        ctx.settings().setRenderKeywordStyle(RenderKeywordStyle.UPPER);

        final Optional<Condition> condition =
                testFilter.generateUsedByDiscoveredAccountsCondition(ctx);

        Assert.assertTrue(condition.isPresent());
        String actualValue = condition.get().toString().replaceAll("\\s", "");
        Assert.assertEquals(actualValue, expectedClause.replaceAll("\\s", ""));

    }
}
