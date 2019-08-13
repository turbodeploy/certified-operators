/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.db;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link EntityTypeTest} checks that {@link EntityType} is created and working as expected.
 */
public class EntityTypeTest {

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
