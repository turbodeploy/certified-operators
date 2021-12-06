package com.vmturbo.common.protobuf.action;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import com.google.protobuf.util.JsonFormat;

import org.apache.commons.io.IOUtils;
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

    /**
     * Test creating the risk description for a move action.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetMoveRisk() throws Exception {
        // ARRANGE
        final ActionDTO.ActionSpec action = readActionFromFile("/move-action");

        Function<Long, String> policyDisplayNameGetter = mock(Function.class);
        Function<Long, String> entityDisplayNameGetter = mock(Function.class);

        when(policyDisplayNameGetter.apply( 285350061265552L)).thenReturn("test policy");
        when(entityDisplayNameGetter.apply( 74123406337744L)).thenReturn("test vm");

        // ACT
        final String risk = RiskUtil.createRiskDescription(action, policyDisplayNameGetter,
                entityDisplayNameGetter);
        final Set<Long> policyIds = RiskUtil.extractPolicyIds(action.getRecommendation());

        // ASSERT
        assertThat(risk, equalTo("\"test vm\" doesn't comply with \"test policy\""));
        assertThat(policyIds, equalTo(Collections.singleton(285350061265552L)));
    }

    /**
     * Test creating the risk description for a reconfigure action.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetReconfigureRisk() throws Exception {
        // ARRANGE
        final ActionDTO.ActionSpec action = readActionFromFile("/reconfigure-action");

        Function<Long, String> policyDisplayNameGetter = mock(Function.class);
        Function<Long, String> entityDisplayNameGetter = mock(Function.class);

        when(policyDisplayNameGetter.apply( 285350065523328L)).thenReturn("test policy");
        when(entityDisplayNameGetter.apply( 74239116498080L)).thenReturn("test vm");

        // ACT
        final String risk = RiskUtil.createRiskDescription(action, policyDisplayNameGetter,
                entityDisplayNameGetter);
        final Set<Long> policyIds = RiskUtil.extractPolicyIds(action.getRecommendation());

        // ASSERT
        assertThat(risk, equalTo("\"test vm\" doesn't comply with \"test policy\""));
        assertThat(policyIds, equalTo(Collections.singleton(285350065523328L)));
    }

    /**
     * Test creating the risk description for a provision action.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetProvisionRisk() throws Exception {
        // ARRANGE
        final ActionDTO.ActionSpec action = readActionFromFile("/provision-action");

        Function<Long, String> policyDisplayNameGetter = mock(Function.class);
        Function<Long, String> entityDisplayNameGetter = mock(Function.class);

        when(policyDisplayNameGetter.apply( 285350074503424L)).thenReturn("test policy");

        // ACT
        final String risk = RiskUtil.createRiskDescription(action, policyDisplayNameGetter,
                entityDisplayNameGetter);
        final Set<Long> policyIds = RiskUtil.extractPolicyIds(action.getRecommendation());

        // ASSERT
        assertThat(risk, equalTo("test policy violation"));
        assertThat(policyIds, equalTo(Collections.singleton(285350074503424L)));
    }

    private static ActionDTO.ActionSpec readActionFromFile(String fileName) throws IOException {
        final ActionDTO.Action.Builder builder = ActionDTO.Action.newBuilder();
        JsonFormat.parser().merge(IOUtils.toString(RiskUtilTest.class
                .getResourceAsStream(fileName), StandardCharsets.UTF_8.name()), builder);
        return ActionDTO.ActionSpec.newBuilder()
                .setRecommendation(builder).build();
    }

}