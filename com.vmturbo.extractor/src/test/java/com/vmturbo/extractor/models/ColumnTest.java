package com.vmturbo.extractor.models;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * Tests of the Column type.
 */
public class ColumnTest {

    /**
     * Test that the type-specific creator methods all properly set the column name.
     */
    @Test
    public void testConvenienceMethodsGetNamesRight() {
        assertThat(Column.stringColumn("x").getName(), is("x"));
        assertThat(Column.longColumn("x").getName(), is("x"));
        assertThat(Column.longArrayColumn("x").getName(), is("x"));
        assertThat(Column.longSetColumn("x").getName(), is("x"));
        assertThat(Column.intColumn("x").getName(), is("x"));
        assertThat(Column.intArrayColumn("x").getName(), is("x"));
        assertThat(Column.shortColumn("x").getName(), is("x"));
        assertThat(Column.doubleColumn("x").getName(), is("x"));
        assertThat(Column.floatColumn("x").getName(), is("x"));
        assertThat(Column.floatArrayColumn("x").getName(), is("x"));
        assertThat(Column.boolColumn("x").getName(), is("x"));
        assertThat(Column.jsonColumn("x").getName(), is("x"));
        assertThat(Column.timestampColumn("x").getName(), is("x"));
    }

    /**
     * Test that the type-specific creation methods all set the correct col types.
     */
    @Test
    public void testConvenienceMethodsGetCorrectColTypes() {
        assertThat(Column.stringColumn("x").getColType(), is(ColType.STRING));
        assertThat(Column.longColumn("x").getColType(), is(ColType.LONG));
        assertThat(Column.longArrayColumn("x").getColType(), is(ColType.LONG_ARRAY));
        assertThat(Column.longSetColumn("x").getColType(), is(ColType.LONG_SET));
        assertThat(Column.intColumn("x").getColType(), is(ColType.INT));
        assertThat(Column.intArrayColumn("x").getColType(), is(ColType.INT_ARRAY));
        assertThat(Column.shortColumn("x").getColType(), is(ColType.SHORT));
        assertThat(Column.doubleColumn("x").getColType(), is(ColType.DOUBLE));
        assertThat(Column.floatColumn("x").getColType(), is(ColType.FLOAT));
        assertThat(Column.floatArrayColumn("x").getColType(), is(ColType.FLOAT_ARRAY));
        assertThat(Column.boolColumn("x").getColType(), is(ColType.BOOL));
        assertThat(Column.jsonColumn("x").getColType(), is(ColType.JSON));
        assertThat(Column.timestampColumn("x").getColType(), is(ColType.TIMESTAMP));
        assertThat(Column.entityTypeColumn("x").getColType(), is(ColType.ENTITY_TYPE));
        assertThat(Column.entityStateColumn("x").getColType(), is(ColType.ENTITY_STATE));
        assertThat(Column.entitySeverityColumn("x").getColType(), is(ColType.ENTITY_SEVERITY));
        assertThat(Column.environmentTypeColumn("x").getColType(), is(ColType.ENVIRONMENT_TYPE));
    }

    /**
     * Test that columns correctly provide associated db types.
     */
    @Test
    public void testThatDbTypeIsCorrectInColumns() {
        // one is enough because prior tests exhaustively make sure column builders use the
        // correct ColType values, and a separate test in ColTypeTest ensures that those
        // values all provide the correct db types
        assertThat(Column.intArrayColumn("x").getDbType(), is(ColType.INT_ARRAY.getPostgresType()));
    }
}
