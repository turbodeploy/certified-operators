package com.vmturbo.common.protobuf;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 *
 */
@RunWith(value = Parameterized.class)
public class ActionDTOUtilTest {

    @Parameter(value=0)
    public String input;

    @Parameter(value=1)
    public String expectedOutput;

    @Parameters
    public static Collection<Object[]> testCases() {
        return Arrays.asList(new Object[][] {
                {"THIS_IS_A_CONSTANT", "This Is A Constant"},
                {"NOUNDERSCORES", "Nounderscores"},
                {"HAS_SPACES AND_UNDERSCORES", "Has Spaces And Underscores"},
                {"Mixed_case_some_lOwEr", "Mixed Case Some Lower"}
        });
    }

    @Test
    public void testCaseConverter() {
        Assert.assertEquals(expectedOutput, ActionDTOUtil.upperUnderScoreToMixedSpaces(input));
    }
}

