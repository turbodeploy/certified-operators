package com.vmturbo.protoc.pojo.gen;

import static org.junit.Assert.assertEquals;

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

/**
 * PrimitiveFieldBitsTest.
 */
public class PrimitiveFieldBitsTest {
    final PrimitiveFieldBits bits = new PrimitiveFieldBits();

    /**
     * testCurrentBitMask.
     */
    @Test
    public void testCurrentBitMask() {
        assertEquals("0x1", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x2", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x4", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x8", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x10", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x20", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x40", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x80", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x100", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x200", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x400", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x800", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x1000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x2000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x4000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x8000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x10000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x20000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x40000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x80000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x100000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x200000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x400000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x800000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x1000000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x2000000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x4000000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x8000000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x10000000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x20000000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x40000000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x80000000", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x1", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x2", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x4", bits.getCurrentBitmask());

        bits.increment();
        assertEquals("0x8", bits.getCurrentBitmask());
    }

    /**
     * Test that that getting all the bitfieldNames accurately reports all.
     */
    @Test
    public void testBitFieldNameForIndex() {
        for (int i = 0; i < PrimitiveFieldBits.BIT_FIELD_POSITIONS; i++) {
            assertEquals("bitField0_", bits.getCurrentBitfieldName());
            bits.increment();
        }
        for (int i = 0; i < PrimitiveFieldBits.BIT_FIELD_POSITIONS; i++) {
            assertEquals("bitField1_", bits.getCurrentBitfieldName());
            bits.increment();
        }

        assertEquals(ImmutableList.of("bitField0_", "bitField1_"),
            bits.getAllBitFieldNames().collect(Collectors.toList()));
    }
}