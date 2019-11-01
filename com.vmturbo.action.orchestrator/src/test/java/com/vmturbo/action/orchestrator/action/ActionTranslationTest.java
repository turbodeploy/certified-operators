package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionTranslationTest {

    private final ActionDTO.Action original = ActionOrchestratorTestUtils
        .createResizeRecommendation(1, 2, CommodityType.VMEM, 1000, 2000);

    private final ActionDTO.Action translated = ActionOrchestratorTestUtils
        .createResizeRecommendation(1, 2, CommodityType.VMEM, 1, 2);

    @Test
    public void testGetTranslationResultOrOriginal() {
        final ActionTranslation translation = new ActionTranslation(original);
        assertEquals(original, translation.getTranslationResultOrOriginal());

        translation.setTranslationSuccess(translated);
        assertEquals(translated, translation.getTranslationResultOrOriginal());
    }

    @Test
    public void testSetTranslationSuccess() {
        final ActionTranslation translation = new ActionTranslation(original);
        assertEquals(Optional.empty(), translation.getTranslatedRecommendation());

        translation.setTranslationSuccess(translated);
        assertEquals(Optional.of(translated), translation.getTranslatedRecommendation());
    }

    @Test
    public void testSetPassthroughTranslationSuccess() {
        final ActionTranslation translation = new ActionTranslation(original);
        assertEquals(Optional.empty(), translation.getTranslatedRecommendation());

        translation.setPassthroughTranslationSuccess();
        assertEquals(Optional.of(original), translation.getTranslatedRecommendation());
    }

    @Test
    public void testSetTranslationFailure() {
        final ActionTranslation translation = new ActionTranslation(original);
        assertEquals(Optional.empty(), translation.getTranslatedRecommendation());

        translation.setTranslationFailure();
        assertEquals(Optional.empty(), translation.getTranslatedRecommendation());
    }
}
