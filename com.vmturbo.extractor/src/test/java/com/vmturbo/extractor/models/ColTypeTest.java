package com.vmturbo.extractor.models;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;

import org.junit.Test;

import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.Severity;

/**
 * Tests for the {@link ColType} class, covering hash and csv functionality.
 */
public class ColTypeTest {

    private TimeZone timezone;

    /**
     * Test that Long coltype operates properly.
     */
    @Test
    public void testLongColType() {
        for (long value : new long[]{Long.MIN_VALUE, Long.MAX_VALUE,
                -1L, 0L, 1L, -1234567890123467890L, -1234567890123467890L}) {
            assertThat(ColType.LONG.fromBytes(ColType.LONG.toBytes(value)), is(value));
        }
        assertThat(ColType.LONG.toCsv(12345L), is("12345"));
    }

    /**
     * Test that Long[] coltype operates properly.
     */
    @Test
    public void testLongArrayColType() {
        for (Long[] value : new Long[][]{new Long[]{1L},
                new Long[]{Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE}}) {
            assertThat((Long[])ColType.LONG_ARRAY.fromBytes(ColType.LONG_ARRAY.toBytes(value)), arrayContaining(value));
        }
        // arrayContaining only matches non-empty, which seems unjustified, so this case can't
        // be done in the loop above... same thing will be true with all the array types
        assertThat((Long[])ColType.LONG_ARRAY.fromBytes(ColType.LONG_ARRAY.toBytes(new Long[0])), emptyArray());
        assertThat(ColType.LONG_ARRAY.toCsv(new Long[]{1L, 2L, null, 3L}), is("\"{1,2,NULL,3}\""));
    }

    /**
     * Test that Long[] set coltype operates properly.
     */
    @Test
    public void testLongSetHashIsOrderIndependent() {
        byte[] hash1 = ColType.LONG_SET.toBytes(new Long[]{-573L, 32L, 1L});
        byte[] hash2 = ColType.LONG_SET.toBytes(new Long[]{1L, -573L, 32L});
        byte[] hash3 = ColType.LONG_SET.toBytes(new Long[]{32L, 1L, -573L});
        assertThat(Collections.singletonList(hash1), contains(hash2));
        assertThat(Collections.singletonList(hash2), contains(hash3));
    }

    /**
     * Test that Integer coltype operates properly.
     */
    @Test
    public void testIntColType() {
        for (int value : new Integer[]{Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, 1, -123457890, 1234567890}) {
            assertThat(ColType.INT.fromBytes(ColType.INT.toBytes(value)), is(value));
        }
        assertThat(ColType.INT.toCsv(12345), is("12345"));
    }

    /**
     * Test that Integer[] coltype operates properly.
     */
    @Test
    public void testIntArrayColType() {
        for (Integer[] value : new Integer[][]{new Integer[]{1},
                new Integer[]{Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}}) {
            assertThat((Integer[])ColType.INT_ARRAY.fromBytes(ColType.INT_ARRAY.toBytes(value)),
                    arrayContaining(value));
        }
        assertThat((Integer[])ColType.INT_ARRAY.fromBytes(ColType.INT_ARRAY.toBytes(new Integer[0])), emptyArray());
        assertThat(ColType.INT_ARRAY.toCsv(new Integer[]{1, 2, null, 3}), is("\"{1,2,NULL,3}\""));
    }

    /**
     * Test that Short coltype operates properly.
     */
    @Test
    public void testShortColType() {
        for (short value : new Short[]{Short.MIN_VALUE, Short.MAX_VALUE, -1, 0, 1, -12345, 12345}) {
            assertThat(ColType.SHORT.fromBytes(ColType.SHORT.toBytes(value)), is(value));
        }
        assertThat(ColType.SHORT.toCsv((short)12345), is("12345"));
    }

    /**
     * Test that Short[] coltype operates properly.
     */
    @Test
    public void testShortArrayColType() {
        for (Short[] value : new Short[][]{new Short[]{1},
                new Short[]{Short.MIN_VALUE, -1, 0, 1, Short.MAX_VALUE}}) {
            assertThat((Short[])ColType.SHORT_ARRAY.fromBytes(ColType.SHORT_ARRAY.toBytes(value)),
                    arrayContaining(value));
        }
        assertThat((Short[])ColType.SHORT_ARRAY.fromBytes(ColType.SHORT_ARRAY.toBytes(new Short[0])), emptyArray());
        assertThat(ColType.SHORT_ARRAY.toCsv(new Short[]{1, 2, null, 3}), is("\"{1,2,NULL,3}\""));
    }

    /**
     * Test that Double coltype operates properly.
     */
    @Test
    public void testDoubleColType() {
        for (double value : new double[]{Double.MIN_VALUE, Double.MAX_VALUE, -1.0, 0.0, 1.0,
                -12134567890.12345, 123567890.12345, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                Double.NaN}) {
            assertThat(Collections.singletonList(ColType.DOUBLE.fromBytes(ColType.DOUBLE.toBytes(value))),
                    contains(value));
        }
        assertThat(ColType.DOUBLE.toCsv(12345.0), is("12345.0"));
    }

    /**
     * Test that Double[] coltype operates properly.
     */
    @Test
    public void testDoubleArrayColType() {
        for (Double[] value : new Double[][]{new Double[]{1.0},
                new Double[]{Double.MIN_VALUE, -1.0, 0.0, 1.0, Double.MAX_VALUE}}) {
            assertThat(Arrays.asList((Double[])ColType.DOUBLE_ARRAY.fromBytes(ColType.DOUBLE_ARRAY.toBytes(value))),
                    contains(value));
        }
        assertThat((Double[])ColType.DOUBLE_ARRAY.fromBytes(ColType.DOUBLE_ARRAY.toBytes(new Double[0])), emptyArray());
        assertThat(ColType.DOUBLE_ARRAY.toCsv(new Double[]{1.0, 2.0, null, 3.0}), is("\"{1.0,2.0,NULL,3.0}\""));
    }

    /**
     * Test that Float coltype operates properly.
     */
    @Test
    public void testFloatColType() {
        for (float value : new float[]{Float.MIN_VALUE, Float.MAX_VALUE, -1.0f, 0.0f, 1.0f,
                -121345.67890f, 1235.67890f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY,
                Float.NaN}) {
            assertThat(Collections.singletonList(ColType.FLOAT.fromBytes(ColType.FLOAT.toBytes(value))), contains(value));
        }
        assertThat(ColType.FLOAT.toCsv(12345.0f), is("12345.0"));
    }

    /**
     * Test that Float[] coltype operates properly.
     */
    @Test
    public void testFloatArrayColType() {
        for (Float[] value : new Float[][]{new Float[]{1f},
                new Float[]{Float.MIN_VALUE, -1f, 0f, 1f, Float.MAX_VALUE}}) {
            assertThat(Arrays.asList((Float[])ColType.FLOAT_ARRAY.fromBytes(ColType.FLOAT_ARRAY.toBytes(value))),
                    contains(value));
        }
        assertThat((Float[])ColType.FLOAT_ARRAY.fromBytes(ColType.FLOAT_ARRAY.toBytes(new Float[0])), emptyArray());
        assertThat(ColType.FLOAT_ARRAY.toCsv(new Float[]{1f, 2f, null, 3f}), is("\"{1.0,2.0,NULL,3.0}\""));
    }

    /**
     * Test that Boolean coltype operates properly.
     */
    @Test
    public void testBooleanColType() {
        for (boolean value : new boolean[]{true, false}) {
            assertThat(Collections.singletonList(ColType.BOOL.fromBytes(ColType.BOOL.toBytes(value))), contains(value));
        }
        assertThat(ColType.BOOL.toCsv(true), is("true"));
        assertThat(ColType.BOOL.toCsv(false), is("false"));
    }

    /**
     * Test that String coltype operates properly.
     */
    @Test
    public void testStringColType() {
        for (String value : new String[]{"", null, "Hello", "恭喜发财"}) {
            assertThat(Collections.singletonList(ColType.STRING.fromBytes(ColType.STRING.toBytes(value))), contains(value));
        }
        assertThat(ColType.STRING.toCsv("here's a quote: \"!"), is("\"here's a quote: \"\"!\""));
        assertThat(ColType.STRING.toCsv(""), is("\"\""));
    }

    /**
     * Test that String[] coltype operates properly.
     */
    @Test
    public void testStringArrayColType() {
        for (String[] value : new String[][]{new String[0], new String[]{""},
                new String[]{"", "Hello", "恭喜发财"}}) {
            assertThat(ColType.STRING_ARRAY.fromBytes(ColType.STRING_ARRAY.toBytes(value)),
                    is(value));
        }
        assertThat(ColType.STRING_ARRAY.toCsv(new String[]{"", "Embedded quote: \" <-", null, "bye"}),
                is("\"{\"\"\"\",\"\"Embedded quote: \"\" <-\"\",NULL,\"\"bye\"\"}\""));
    }

    /**
     * Test that JSON coltype operates properly.
     */
    @Test
    public void testJsonColTypes() {
        for (String value : new String[]{"", "Hello", "恭喜发财"}) {
            assertThat(ColType.JSON.fromBytes(ColType.JSON.toBytes(new JsonString(value))).toString(), is(value));
        }
        assertThat(ColType.JSON.toCsv("{\"a\": 1234, [1,2,3]}"), is("\"{\"\"a\"\": 1234, [1,2,3]}\""));
    }

    /**
     * Test that Timezone coltype operates properly.
     */
    @Test
    public void testTimestampColTypes() {
        for (Timestamp value : new Timestamp[]{new Timestamp(0L),
                                               new Timestamp(Long.MAX_VALUE),
                                               Timestamp.from(Instant.EPOCH),
                                               Timestamp.from(Instant.MAX),
                                               Timestamp.from(Instant.MIN),
                                               Timestamp.from(Instant.now()) }) {
            assertThat(ColType.TIMESTAMP.fromBytes(ColType.TIMESTAMP.toBytes(value)), is(value));
        }
        assertThat(ColType.TIMESTAMP.toCsv(new Timestamp(0L)), is(new Timestamp(0L).toString()));
    }

    /**
     * Test that null columns values of all types are handled properly.
     */
    @Test
    public void testNullColumnValues() {
        assertThat(ColType.INT.fromBytes(ColType.INT.toBytes(null)), is(nullValue()));
        assertThat(ColType.INT.toCsv(null), is(""));
    }

    /**
     * Test that entity_type coltype operates properly.
     */
    @Test
    public void testEntityTypeColType() {
        for (EntityType value : EntityType.values()) {
            assertThat(Collections.singletonList(ColType.ENTITY_TYPE.fromBytes(
                    ColType.ENTITY_TYPE.toBytes(value))), contains(value));
            assertThat(ColType.ENTITY_TYPE.toCsv(value), is(value.getLiteral()));
        }
    }

    /**
     * Test that entity_state coltype operates properly.
     */
    @Test
    public void testEntityStateColType() {
        for (EntityState value : EntityState.values()) {
            assertThat(Collections.singletonList(ColType.ENTITY_STATE.fromBytes(
                    ColType.ENTITY_STATE.toBytes(value))), contains(value));
            assertThat(ColType.ENTITY_STATE.toCsv(value), is(value.getLiteral()));
        }
    }

    /**
     * Test that entity_severity coltype operates properly.
     */
    @Test
    public void testEntitySeverityColType() {
        for (Severity value : Severity.values()) {
            assertThat(Collections.singletonList(ColType.SEVERITY.fromBytes(
                    ColType.SEVERITY.toBytes(value))), contains(value));
            assertThat(ColType.SEVERITY.toCsv(value), is(value.getLiteral()));
        }
    }

    /**
     * Test that environment_type coltype operates properly.
     */
    @Test
    public void testEnvironmentTypeColType() {
        for (EnvironmentType value : EnvironmentType.values()) {
            assertThat(Collections.singletonList(ColType.ENVIRONMENT_TYPE.fromBytes(
                    ColType.ENVIRONMENT_TYPE.toBytes(value))), contains(value));
            assertThat(ColType.ENVIRONMENT_TYPE.toCsv(value), is(value.getLiteral()));
        }
    }

    /**
     * Test that the db types configured for all the column types are correct.
     */
    @Test
    public void testThatDbTypesAreCorrect() {
        assertThat(ColType.STRING.getPostgresType(), is("varchar"));
        assertThat(ColType.LONG.getPostgresType(), is("int8"));
        assertThat(ColType.LONG_ARRAY.getPostgresType(), is("int8[]"));
        assertThat(ColType.LONG_SET.getPostgresType(), is("int8[]"));
        assertThat(ColType.INT.getPostgresType(), is("int4"));
        assertThat(ColType.INT_ARRAY.getPostgresType(), is("int4[]"));
        assertThat(ColType.SHORT.getPostgresType(), is("int2"));
        assertThat(ColType.SHORT_ARRAY.getPostgresType(), is("int2[]"));
        assertThat(ColType.DOUBLE.getPostgresType(), is("float8"));
        assertThat(ColType.DOUBLE_ARRAY.getPostgresType(), is("float8[]"));
        assertThat(ColType.FLOAT.getPostgresType(), is("float4"));
        assertThat(ColType.FLOAT_ARRAY.getPostgresType(), is("float4[]"));
        assertThat(ColType.BOOL.getPostgresType(), is("boolean"));
        assertThat(ColType.JSON.getPostgresType(), is("jsonb"));
        assertThat(ColType.TIMESTAMP.getPostgresType(), is("timestamptz"));
        assertThat(ColType.ENTITY_TYPE.getPostgresType(), is("entity_type"));
        assertThat(ColType.ENTITY_STATE.getPostgresType(), is("entity_state"));
        assertThat(ColType.SEVERITY.getPostgresType(), is("severity"));
        assertThat(ColType.ENVIRONMENT_TYPE.getPostgresType(), is("environment_type"));
    }
}
