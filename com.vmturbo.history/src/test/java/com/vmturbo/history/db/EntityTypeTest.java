package com.vmturbo.history.db;

import org.assertj.core.api.SoftAssertions;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests to sanity-check information in the EntityType enum.
 */
public class EntityTypeTest extends Assert {

    /**
     * Tables appearing in {@link EntityType#ROLLED_UP_ENTITIES} must be declared with all four
     * varieties of entity stats table: latest, hourly, daily, monthly.
     */
    @Test
    public void rolledUpTablesAreComplete() {
        // report all violations in this test, not just the first one encountered
        final SoftAssertions softly = new SoftAssertions();
        EntityType.ROLLED_UP_ENTITIES.forEach(type -> {
            softly.assertThat(type.getLatestTable())
                .as(rollupTableDescription(type, "latest"))
                .isNotNull();
            softly.assertThat(type.getHourTable())
                .as(rollupTableDescription(type, "hourly"))
                .isNotNull();
            softly.assertThat(type.getDayTable())
                .as(rollupTableDescription(type, "daily"))
                .isNotNull();
            softly.assertThat(type.getMonthTable())
                .as(rollupTableDescription(type, "monthly"))
                .isNotNull();
        });
        softly.assertAll();
    }

    private String rollupTableDescription(final EntityType type, final String variant) {
        return String.format("Entity type %s in ROLLED_UP_ENTITIES has a %s table",
            type, variant);
    }

    /**
     * Checks that special mapping case {@link com.vmturbo.components.common.utils.StringConstants#DATA_CENTER}
     * is correctly mapping to {@link EntityType#PHYSICAL_MACHINE}.
     */
    @Test
    public void checkThatDataCenterEntityTypeMappedToPhysicalMachineTables() {
        Assert.assertThat(EntityType.get(EntityType.DATA_CENTER.getClsName()),
                CoreMatchers.is(EntityType.PHYSICAL_MACHINE));
    }

    /**
     * Checks that spend tables do not participate in table to entity type mapping.
     */
    @Test
    public void checkAbsenceOfSpendTables() {
        EntityType.TABLE_TO_SPEND_ENTITY_MAP.keySet().forEach(table -> {
            final EntityType entityType = EntityType.fromTable(table);
            Assert.assertTrue(
                    String.format("Table '%s' was associated with non-spend entity type '%s'",
                            table.toString(), entityType),
                    entityType.isSpendEntity());
        });
    }
}
