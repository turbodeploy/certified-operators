package com.vmturbo.sql.utils.jooq;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import org.junit.Test;

/**
 * Tests of the {@link UpsertBuilder} class.
 */
public class UpsertBuilderTest {

    private final Table<?> target = new MyTableImpl("dest",
            DSL.field("pk1", String.class), DSL.field("pk2", Long.class),
            DSL.field("x", String.class), DSL.field("y", String.class),
            DSL.field("xd1", String.class), DSL.field("yd1", String.class),
            DSL.field("xd2", String.class), DSL.field("yd2", String.class),
            DSL.field("max", Double.class), DSL.field("min", Double.class),
            DSL.field("avg", Double.class), DSL.field("n", Integer.class),
            DSL.field("z", String.class));
    private final Table<?> source = new MyTableImpl("source",
            DSL.field("pk1", String.class), DSL.field("pk2", Long.class),
            DSL.field("x", String.class), DSL.field("y", String.class),
            DSL.field("xd1", String.class), DSL.field("yd1", String.class),
            // xd2 and yd2 do not appear in source table
            DSL.field("max", Double.class), DSL.field("min", Double.class),
            DSL.field("avg", Double.class), DSL.field("n", Integer.class),
            DSL.field("z", String.class));

    // get target field instances
    private final Field<String> pk1 = target.field("pk1", String.class);
    private final Field<Long> pk2 = target.field("pk2", Long.class);
    private final Field<String> x = target.field("x", String.class);
    private final Field<String> y = target.field("y", String.class);
    private final Field<String> xd1 = target.field("xd1", String.class);
    private final Field<String> yd1 = target.field("yd1", String.class);
    private final Field<String> xd2 = target.field("xd2", String.class);
    private final Field<String> yd2 = target.field("yd2", String.class);
    private final Field<Double> max = target.field("max", Double.class);
    private final Field<Double> min = target.field("min", Double.class);
    private final Field<Double> avg = target.field("avg", Double.class);
    private final Field<Integer> n = target.field("n", Integer.class);
    private final Field<String> z = target.field("z", String.class);

    private final UpsertBuilder upsert = new UpsertBuilder()
            .withTargetTable(target)
            .withSourceTable(source)
            .withInsertFields(x, y, xd1, yd1, xd2, yd2, max, min, avg, n, z, pk1, pk2)
            .withInsertValue(x, DSL.inline("hello"))
            .withInsertValue(y, "good-bye")
            .withInsertValueDefault(xd2, "xdef")
            .withInsertValueDefault(yd1, DSL.inline("ydef"))
            .withInsertValueDefault(xd2, "xdef")
            .withInsertValueDefault(yd2, DSL.inline("ydef"))
            .withSourceCondition(source.field("pk1", String.class).gt("abc"))
            .withSourceCondition(source.field("pk2", Long.class).mod(10).eq(1L))
            .withUpdateValue(max, UpsertBuilder::max)
            .withUpdateValue(min, UpsertBuilder::min)
            .withUpdateValue(n, UpsertBuilder::sum)
            .withUpdateValue(avg, UpsertBuilder.avg(n))
            .withUpdateValue(x, UpsertBuilder::inserted)
            .withUpdateValue(z, UpsertBuilder.inline("wow"));

    /**
     * Render our upsert statement and make sure it's as expected for MARIADB dialect.
     */
    @Test
    public void testUpsertIsCorrectInMariaDbDialect() {
        assertThat(upsert.getUpsert(dslFor(SQLDialect.MARIADB)).getSQL(), is(flatten(
                "insert into dest (x, y, xd1, yd1, xd2, yd2, max, min, avg, n, z, pk1, pk2)",
                "select 'hello', 'good-bye', source.xd1, source.yd1, 'xdef', 'ydef', source.max, source.min, source.avg, source.n, source.z, source.pk1, source.pk2",
                "from source",
                "where (source.pk1 > 'abc' and mod(source.pk2, 10) = 1)",
                "on duplicate key update",
                "  dest.max = case when dest.max > values(max) then dest.max else values(max) end,",
                "  dest.min = case when dest.min < values(min) then dest.min else values(min) end,",
                "  dest.n = (dest.n + values(n)),",
                "  dest.avg = (((dest.avg * dest.n) + (values(avg) * values(n))) / (dest.n + values(n))),",
                "  dest.x = values(x),",
                "  dest.z = 'wow'")));
        assertThat(upsert
                .withConflictColumns(pk1)
                .getUpsert(dslFor(SQLDialect.MARIADB)).getSQL(), is(flatten(
                "insert into dest (x, y, xd1, yd1, xd2, yd2, max, min, avg, n, z, pk1, pk2)",
                "select 'hello', 'good-bye', source.xd1, source.yd1, 'xdef', 'ydef', source.max, source.min, source.avg, source.n, source.z, source.pk1, source.pk2",
                "from source",
                "where (source.pk1 > 'abc' and mod(source.pk2, 10) = 1)",
                "on duplicate key update",
                "  dest.max = case when dest.max > values(max) then dest.max else values(max) end,",
                "  dest.min = case when dest.min < values(min) then dest.min else values(min) end,",
                "  dest.n = (dest.n + values(n)),",
                "  dest.avg = (((dest.avg * dest.n) + (values(avg) * values(n))) / (dest.n + values(n))),",
                "  dest.x = values(x),",
                "  dest.z = 'wow'")));
    }

    /**
     * Render our upsert statement and make sure it's as expected for POSTGRES dialect.
     */
    @Test
    public void testUpsertIsCorrectInPostgresDialect() {
        assertThat(upsert.getUpsert(dslFor(SQLDialect.POSTGRES)).getSQL(), is(flatten(
                "insert into dest (x, y, xd1, yd1, xd2, yd2, max, min, avg, n, z, pk1, pk2)",
                "select 'hello', 'good-bye', source.xd1, source.yd1, 'xdef', 'ydef', source.max, source.min, source.avg, source.n, source.z, source.pk1, source.pk2",
                "from source",
                "where (source.pk1 > 'abc' and mod(source.pk2, 10) = 1)",
                "on conflict ([unknown primary key]) do update",
                "set max = case when dest.max > excluded.max then dest.max else excluded.max end,",
                "  min = case when dest.min < excluded.min then dest.min else excluded.min end,",
                "  n = (dest.n + excluded.n),",
                "  avg = (((dest.avg * dest.n) + (excluded.avg * excluded.n)) / (dest.n + excluded.n)),",
                "  x = excluded.x,",
                "  z = 'wow'")));
        assertThat(upsert
                .withConflictColumns(pk1)
                .getUpsert(dslFor(SQLDialect.POSTGRES)).getSQL(), is(flatten(
                "insert into dest (x, y, xd1, yd1, xd2, yd2, max, min, avg, n, z, pk1, pk2)",
                "select 'hello', 'good-bye', source.xd1, source.yd1, 'xdef', 'ydef', source.max, source.min, source.avg, source.n, source.z, source.pk1, source.pk2",
                "from source",
                "where (source.pk1 > 'abc' and mod(source.pk2, 10) = 1)",
                "on conflict (pk1) do update",
                "set max = case when dest.max > excluded.max then dest.max else excluded.max end,",
                "  min = case when dest.min < excluded.min then dest.min else excluded.min end,",
                "  n = (dest.n + excluded.n),",
                "  avg = (((dest.avg * dest.n) + (excluded.avg * excluded.n)) / (dest.n + excluded.n)),",
                "  x = excluded.x,",
                "  z = 'wow'")));
    }

    private <T> Field<T> tableField(Table<?> table, String name, Class<T> type) {
        return DSL.field(DSL.name(table.getQualifiedName(), DSL.name(name)), type);
    }

    private DSLContext dslFor(SQLDialect dialect) {
        DSLContext dsl = DSL.using(dialect);
        dsl.settings().withRenderQuotedNames(RenderQuotedNames.NEVER)
                .withRenderSchema(false)
                .withParamType(ParamType.INLINED);
        return dsl;
    }

    private String flatten(String... lines) {
        return String.join(" ", lines).trim().replaceAll("\\s+", " ");
    }

    /**
     * Simple exension of TableImpl that installs given fields as table fields.
     *
     * <p>This is needed for the #withInsertValueDefault methods, which requires the source
     * table to know its fields.</p>
     */
    private static class MyTableImpl extends TableImpl<Record> {
        /**
         * Create a new instance.
         *
         * @param name   table name
         * @param fields table fields
         */
        MyTableImpl(String name, Field<?>... fields) {
            super(DSL.name(name));
            for (Field<?> field : fields) {
                createField(field.getQualifiedName(), field.getDataType(), this);
            }
        }
    }
}