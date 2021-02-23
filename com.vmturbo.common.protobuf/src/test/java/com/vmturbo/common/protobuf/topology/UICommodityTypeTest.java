package com.vmturbo.common.protobuf.topology;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.StringUtil;

/**
 *
 */
public class UICommodityTypeTest {

    /**
     * Validate that the generated display names are what we expect for some of the unusual commodity
     * type strings.
     */
    @RunWith(Parameterized.class)
    public static class DisplayNameTests {
        @Parameters(name = "{index}: input {0}, expectedOutput {1}")
        public static Collection<Object[]> cases() {
            return Arrays.asList(new Object[][]{
                    {UICommodityType.STORAGE_AMOUNT, "Storage Amount"},
                    {UICommodityType.CPU_ALLOCATION, "CPU Allocation"},
                    {UICommodityType.DISK_ARRAY_ACCESS, "Disk Array Access"},
                    {UICommodityType.VCPU, "VCPU"},
                    {UICommodityType.VDC, "VDC"},
            });
        }

        @Parameter(0)
        public UICommodityType commodity;

        @Parameter(1)
        public String expectedDisplayName;

        @Test
        public void validateDisplayName() {
            Assert.assertEquals(expectedDisplayName, commodity.displayName());
        }
    }
}
