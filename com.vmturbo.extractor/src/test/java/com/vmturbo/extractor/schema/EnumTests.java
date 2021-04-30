package com.vmturbo.extractor.schema;

import static com.vmturbo.extractor.schema.enums.EntityType.BILLING_FAMILY;
import static com.vmturbo.extractor.schema.enums.EntityType.COMPUTE_CLUSTER;
import static com.vmturbo.extractor.schema.enums.EntityType.GROUP;
import static com.vmturbo.extractor.schema.enums.EntityType.K8S_CLUSTER;
import static com.vmturbo.extractor.schema.enums.EntityType.RESOURCE_GROUP;
import static com.vmturbo.extractor.schema.enums.EntityType.STORAGE_CLUSTER;
import static com.vmturbo.extractor.schema.enums.EntityType._NONE_;
import static com.vmturbo.extractor.schema.enums.MetricType.CPU_HEADROOM;
import static com.vmturbo.extractor.schema.enums.MetricType.CPU_READY;
import static com.vmturbo.extractor.schema.enums.MetricType.MEM_HEADROOM;
import static com.vmturbo.extractor.schema.enums.MetricType.STORAGE_HEADROOM;
import static com.vmturbo.extractor.schema.enums.MetricType.TOTAL_HEADROOM;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.extractor.models.Constants;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.enums.TerminalState;
import com.vmturbo.extractor.topology.mapper.GroupMappers;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Tests related to our DB enum types.
 *
 * <p>The individual tests mostly compare the set of value (names) in one of the DB types to the
 * set of values from the corresponding source type, reporting errors if there are any missing or
 * extra values in the DB type. The tests accommodate exception lists of permitted missing and extra
 * values.</p>
 */
public class EnumTests {

    private static final Set<EntityType> OK_EXTRA_ENTITY_TYPES =
            ImmutableSet.of(GROUP, RESOURCE_GROUP, COMPUTE_CLUSTER, K8S_CLUSTER, STORAGE_CLUSTER,
                    BILLING_FAMILY, _NONE_);
    private static final Set<EntityDTO.EntityType> OK_MISSING_ENTITY_TYPES =
            ImmutableSet.of(EntityDTO.EntityType.UNKNOWN);

    private static final Set<CommodityType> OK_MISSING_METRIC_TYPES =
            Sets.difference(ImmutableSet.copyOf(CommodityType.values()),
                    Constants.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST);
    private static final Set<MetricType> OK_EXTRA_METRIC_TYPES =
            ImmutableSet.of(CPU_HEADROOM, MEM_HEADROOM, STORAGE_HEADROOM, TOTAL_HEADROOM, CPU_READY);

    private static final ImmutableSet<ActionDTO.ActionState> OK_MISSING_TERMINAL_STATE =
            ImmutableSet.of(ActionDTO.ActionState.READY, ActionDTO.ActionState.CLEARED,
                    ActionDTO.ActionState.IN_PROGRESS, ActionDTO.ActionState.ACCEPTED,
                    ActionDTO.ActionState.FAILING, ActionDTO.ActionState.QUEUED,
                    ActionDTO.ActionState.PRE_IN_PROGRESS, ActionDTO.ActionState.POST_IN_PROGRESS,
                    ActionDTO.ActionState.REJECTED);
    private static final ImmutableSet<CostCategory> OK_EXTRA_COST_CATEGORIES =
            ImmutableSet.of(CostCategory.TOTAL);
    private static final ImmutableSet<CostSource> OK_EXTRA_COST_SOURCES =
            ImmutableSet.of(CostSource.TOTAL);


    /**
     * Test that all we have all correct values for `entity_type`.
     */
    @Test
    public void testEntityTypeEnum() {
        testEnum(EntityType.class, EntityDTO.EntityType.class,
                OK_MISSING_ENTITY_TYPES, OK_EXTRA_ENTITY_TYPES);
    }

    /**
     * Test that group types are all covered in our `entity_type` enum.
     *
     * <p>Since most of these use alternative names, we use the internal mapper to try to
     * obtain the correct `enum_type` value for each group type. The mapper will throw {@link
     * IllegalArgumentException} if an unmapped value is presented, and we'll rely on that to cause
     * the test to fail.</p>
     */
    @Test
    public void testGroupTypesInEntityTypeEnum() {
        for (final GroupType groupType : GroupType.values()) {
            GroupMappers.mapGroupTypeToEntityType(groupType);
        }
    }

    /**
     * Test that we have the correct set of `environment_type` values.
     */
    @Test
    public void testEnvironmentTypeEnum() {
        testEnum(EnvironmentType.class, EnvironmentTypeEnum.EnvironmentType.class, null, null);
    }

    /**
     * Test that we have the correct `entity_state` values.
     */
    @Test
    public void testEntityStateEnum() {
        testEnum(EntityState.class, TopologyDTO.EntityState.class, null, null);
    }

    /**
     * Test that we have the correct `action_category` values.
     */
    @Test
    public void testActionCategoryEnum() {
        testEnum(ActionCategory.class, ActionDTO.ActionCategory.class, null, null);
    }

    /**
     * Test that we have the correct `action_state` values.
     */
    @Test
    public void testActionStateEnum() {
        testEnum(ActionState.class, ActionDTO.ActionState.class, null, null);
    }

    /**
     * Test that we have the correct `action_type` values.
     */
    @Test
    public void testActionTypeEnum() {
        testEnum(ActionType.class, ActionDTO.ActionType.class, null, null);
    }

    /**
     * Test that we have the correct `severity` values.
     */
    @Test
    public void testSeverityEnum() {
        testEnum(Severity.class, ActionDTO.Severity.class, null, null);
    }

    /**
     * Test that our `terminal_state` enum is a subset of {@link ActionDTO.ActionState}.
     */
    @Test
    public void testTerminalStateEnum() {
        testEnum(TerminalState.class, ActionDTO.ActionState.class, OK_MISSING_TERMINAL_STATE, null);
    }

    /**
     * Test that we have the correct `metric_type` values.
     *
     * <p>Since we have an internal whitelist of allowed values, we allow all other
     * {@link CommodityType} values to be missing. We also have a few extra values, e.g. for cluster
     * headroom.</p>
     */
    @Test
    public void testMetricTypeEnum() {
        testEnum(MetricType.class, CommodityType.class,
                OK_MISSING_METRIC_TYPES, OK_EXTRA_METRIC_TYPES);
    }

    /**
     * Test that we have the correct `cost_category` values.
     *
     * <p>We include a `TOTAL` value in the DB enum for convenience.</p>
     */
    @Test
    public void testCostCategoryEnum() {
        testEnum(CostCategory.class, Cost.CostCategory.class, null, OK_EXTRA_COST_CATEGORIES);
    }

    /**
     * Test that we have the correct `cost_source` values.
     */
    @Test
    public void testCostSourceEnum() {
        testEnum(CostSource.class, Cost.CostSource.class, null, OK_EXTRA_COST_SOURCES);
    }

    /**
     * Test that two given enum classes have the same value names, not counting expected missing and
     * extra values.
     *
     * @param dbEnumClass     jOOQ generated {@link Enum} class corresponding to a DB enum type
     * @param srcEnumClass    java {@link Enum} class that the db type is expected to mimic
     * @param okMissingValues values of the source type that are intentionally omitted from the db
     *                        type
     * @param okExtraValues   legitimate values of the db type that do not correspond to any source
     *                        values
     * @param <DbEnumT>       the db enum type
     * @param <SrcEnumT>      the source enum type
     */
    private <DbEnumT extends Enum<DbEnumT>, SrcEnumT extends Enum<SrcEnumT>> void testEnum(
            Class<DbEnumT> dbEnumClass, Class<SrcEnumT> srcEnumClass,
            Set<SrcEnumT> okMissingValues, Set<DbEnumT> okExtraValues) {
        final Set<String> dbEnumNames = Arrays.stream(dbEnumClass.getEnumConstants())
                .map(DbEnumT::name)
                .collect(Collectors.toSet());
        final Set<String> srcEnumNames = Arrays.stream(srcEnumClass.getEnumConstants())
                .map(SrcEnumT::name)
                .collect(Collectors.toSet());
        if (!dbEnumNames.equals(srcEnumNames)) {
            final Set<String> okMissingNames = okMissingValues != null
                    ? okMissingValues.stream().map(SrcEnumT::name).collect(Collectors.toSet())
                    : Collections.emptySet();
            final SetView<String> missing = Sets.difference(Sets.difference(srcEnumNames, dbEnumNames), okMissingNames);
            final Set<String> okExtraNames = okExtraValues != null
                    ? okExtraValues.stream().map(DbEnumT::name).collect(Collectors.toSet())
                    : Collections.emptySet();
            final SetView<String> extra = Sets.difference(Sets.difference(dbEnumNames, srcEnumNames), okExtraNames);
            final String missingMsg = String.format("Values in source enum %s, not in DB enum %s",
                    srcEnumClass.getSimpleName(), dbEnumClass.getSimpleName());
            assertThat(missingMsg,
                    missing, is(empty()));
            final String extraMsg = String.format("Values in DB enum %s, not in source enum %s",
                    dbEnumClass.getName(), srcEnumClass.getName());
            assertThat(extraMsg,
                    extra, is(empty()));
        } else {
            assertThat(dbEnumNames, equalTo(srcEnumNames));
        }
    }
}
