package com.vmturbo.sql.utils.jooq;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;

import org.apache.commons.codec.digest.DigestUtils;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.Test;

/**
 * Tests for the {@link ProxyKey} class.
 */
public class ProxyKeyTest {

    private static final Field<String> fString = DSL.field("string", String.class);
    private static final Field<Timestamp> fTimestamp = DSL.field("timestamp", Timestamp.class);
    private static final Field<Double> fDouble = DSL.field("double", Double.class);
    private static final Field<Long> fLong = DSL.field("long", Long.class);
    private static final Table<?> t = DSL.select(fString, fTimestamp, fDouble, fLong)
            .asTable();

    /**
     * Test that proxy keys are correct when all fields are non-null.
     */
    @Test
    public void testThatProxyKeyWorksWithoutNulls() {
        Record r = t.newRecord()
                .with(fString, "x")
                .with(fTimestamp, new Timestamp(0L))
                .with(fDouble, 1.0)
                .with(fLong, 100L);
        assertThat(ProxyKey.getKey(r, fString, fTimestamp, fDouble, fLong),
                is("x" + new Timestamp(0L).toString() + "1.0" + "100"));
    }

    /**
     * Test that proxy key is correct when all fields are null.
     */
    @Test
    public void testThatProxyKeyWorksWithAllNulls() {
        assertThat(ProxyKey.getKey(t.newRecord(), fString, fTimestamp, fDouble, fLong),
                is("----"));
    }

    /**
     * Test that a mixed case - some null, some non-nulls - yields a good key value.
     */
    @Test
    public void testThatProxyKeyWorksWithSomeNulls() {
        Record r = t.newRecord()
                .with(fString, "x")
                .with(fLong, 100L);
        assertThat(ProxyKey.getKey(r, fString, fTimestamp, fDouble, fLong), is("x--100"));
    }

    /**
     * Make sure that a proxy key with no constituent works.
     */
    @Test
    public void testThatProxyKeyWorksWithNoFields() {
        assertThat(ProxyKey.getKey(t.newRecord()), is(""));
    }

    /**
     * Test that keys are correctly rendered as bytes.
     */
    @Test
    public void testThatProxyKeyAsByteWorks() {
        Record r = t.newRecord()
                .with(fString, "x")
                .with(fTimestamp, new Timestamp(0L))
                .with(fDouble, 1.0)
                .with(fLong, 100L);
        assertThat(ProxyKey.getKeyAsBytes(r, fString, fTimestamp, fDouble, fLong),
                equalTo(DigestUtils.md5("x" + new Timestamp(0L).toString() + "1.0" + "100")));
    }

    /**
     * Test that keys are correctly rendered as hex strings.
     */
    @Test
    public void testThatProxyKeyAsHexWorks() {
        Record r = t.newRecord()
                .with(fString, "x")
                .with(fTimestamp, new Timestamp(0L))
                .with(fDouble, 1.0)
                .with(fLong, 100L);
        assertThat(ProxyKey.getKeyAsHex(r, fString, fTimestamp, fDouble, fLong),
                is(DigestUtils.md5Hex("x" + new Timestamp(0L).toString() + "1.0" + "100")));
    }

    /**
     * Test that proxy keys are correct with "fields" that are actaully jOOQ-inlined values.
     */
    @Test
    public void testThatInlinedValuesWorkAsFields() {
        Record r = t.newRecord()
                .with(fString, "x")
                .with(fTimestamp, new Timestamp(0L))
                .with(fDouble, 1.0)
                .with(fLong, 100L);
        assertThat(ProxyKey.getKeyAsHex(r, DSL.inline("xyzzy"), fTimestamp, fDouble, fLong),
                is(DigestUtils.md5Hex("xyzzy" + new Timestamp(0L).toString() + "1.0" + "100")));
    }
}
