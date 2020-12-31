package com.vmturbo.common.protobuf.action;

import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link RiskUtil} class.
 */
public class RiskUtilTest {

    /**
     * Tests the {@link RiskUtil#translateExplanation(String, Function)} method.
     */
    @Test
    public void testTranslateExplanation() {

        String noTranslationNeeded = "Simple string";
        Assert.assertEquals("Simple string", RiskUtil.translateExplanation(noTranslationNeeded,
            oid -> "displayName"));

        // test display name
        String translateName = ActionDTOUtil.TRANSLATION_PREFIX + "The entity name is "
            + ActionDTOUtil.createTranslationBlock(1, ActionDTOUtil.EntityField.DISPLAY_NAME, "default");
        Assert.assertEquals("The entity name is Test Entity",
            RiskUtil.translateExplanation(translateName, oid -> "Test Entity"));

        // test fallback value
        String testFallback = ActionDTOUtil.TRANSLATION_PREFIX + "The entity madeup field is "
            + ActionDTOUtil.createTranslationBlock(1, ActionDTOUtil.EntityField.DISPLAY_NAME, "fallback value");
        Assert.assertEquals("The entity madeup field is fallback value",
            RiskUtil.translateExplanation(testFallback, oid -> null));
        // test blank fallback value
        String testBlankFallback = ActionDTOUtil.TRANSLATION_PREFIX + "The entity madeup field is "
            + ActionDTOUtil.createTranslationBlock(1, ActionDTOUtil.EntityField.DISPLAY_NAME, "");
        Assert.assertEquals("The entity madeup field is ",
            RiskUtil.translateExplanation(testBlankFallback, oid -> null));

        // test block at start of string
        String testStart = ActionDTOUtil.TRANSLATION_PREFIX
            + ActionDTOUtil.createTranslationBlock(1,
            ActionDTOUtil.EntityField.DISPLAY_NAME, "default") + " and stuff";
        Assert.assertEquals("Test Entity and stuff", RiskUtil.translateExplanation(testStart,
            oid -> "Test Entity"));

    }

}