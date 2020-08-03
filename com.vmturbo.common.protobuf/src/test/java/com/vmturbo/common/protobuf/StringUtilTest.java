package com.vmturbo.common.protobuf;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 *
 */
public class StringUtilTest {

    @RunWith(Parameterized.class)
    public static class CamelCaseStringConversionTests {
        @Parameters(name="{index}: input {0}, expectedOutput {1}")
        public static Collection<Object[]> cases() {
            return Arrays.asList(new Object[][] {
                    {"lowerUpper", "lower Upper"},
                    {"March5", "March 5"},
                    {"PDFLoader", "PDF Loader"},
            });
        }

        @Parameter(0)
        public String input;

        @Parameter(1)
        public String expectedOutput;

        @Test
        public void testCamelCaseToWordsConverter() {
            Assert.assertEquals(expectedOutput, StringUtil.getSpaceSeparatedWordsFromCamelCaseString(input));
        }
    }

    @RunWith(Parameterized.class)
    public static class BeautifyStringTests {
        @Parameters(name="{index}: input {0}, expectedOutput {1}")
        public static Collection<Object[]> cases() {
            return Arrays.asList(new Object[][] {
                    {"VIRTUAL_MACHINE", "Virtual Machine"},
                    {"SUSPEND", "Suspend"},
                    {"IO_MODULE", "Io Module"},
            });
        }

        @Parameter(0)
        public String input;

        @Parameter(1)
        public String expectedOutput;

        @Test
        public void testCamelCaseToWordsConverter() {
            Assert.assertEquals(expectedOutput, StringUtil.beautifyString(input));
        }
    }
}
