package com.vmturbo.stitching.journal;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.JsonDiffField.DifferenceType;

public class SpaceRendererTest {
    @Test
    public void testPrettyRenderEmpty() {
        assertEquals("", SpaceRenderer.newRenderer(0, 0, FormatRecommendation.PRETTY).render());
    }

    @Test
    public void testPrettyRenderInitialBlanks() {
        assertEquals("  ", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.PRETTY).render());
    }

    @Test
    public void testPrettyRenderOffset() {
        assertEquals("    ", SpaceRenderer.newRenderer(2, 2, FormatRecommendation.PRETTY).render());
    }

    @Test
    public void testPrettyRenderAddition() {
        assertEquals("  ++", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.PRETTY)
            .childRenderer(DifferenceType.ADDITION)
            .render());
    }

    @Test
    public void testPrettyRenderRemoval() {
        assertEquals("  --", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.PRETTY)
            .childRenderer(DifferenceType.REMOVAL)
            .render());
    }

    @Test
    public void testPrettySpace() {
        assertEquals(" ", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.PRETTY).space());
    }

    @Test
    public void testPrettyPrettyNewLine() {
        assertEquals("\n", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.PRETTY).prettyNewLine());
    }

    @Test
    public void testPrettyMandatoryNewLine() {
        assertEquals("\n", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.PRETTY).mandatoryNewLine());
    }

    @Test
    public void testCompactRenderEmpty() {
        assertEquals("", SpaceRenderer.newRenderer(0, 0, FormatRecommendation.COMPACT).render());
    }

    @Test
    public void testCompactRenderInitialBlanks() {
        assertEquals("  ", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.COMPACT).render());
    }

    @Test
    public void testCompactRenderInitialBlanksAndContinuation() {
        final SpaceRenderer renderer = SpaceRenderer.newRenderer(2, 0, FormatRecommendation.COMPACT);
        assertEquals("  ", renderer.render());
        assertEquals("", renderer.render());
    }

    @Test
    public void testCompactRenderOffset() {
        final SpaceRenderer renderer = SpaceRenderer.newRenderer(2, 2, FormatRecommendation.COMPACT);
        assertEquals("  ", renderer.render());
        assertEquals("", renderer.render());
    }

    @Test
    public void testCompactRenderAddition() {
        final SpaceRenderer renderer = SpaceRenderer.newRenderer(2, 2, FormatRecommendation.COMPACT);
        assertEquals("  ", renderer.render());
        assertEquals("++", renderer.childRenderer(DifferenceType.ADDITION).render());
    }

    @Test
    public void testCompactRenderAdditionMultipleDescendants() {
        final SpaceRenderer renderer = SpaceRenderer.newRenderer(2, 2, FormatRecommendation.COMPACT);
        assertEquals("  ", renderer.render());
        final SpaceRenderer child = renderer.childRenderer(DifferenceType.ADDITION);
        assertEquals("++", child.render());
        final SpaceRenderer grandChild = child.childRenderer(DifferenceType.ADDITION);
        assertEquals("", grandChild.render());
    }

    @Test
    public void testCompactRenderRemoval() {
        final SpaceRenderer renderer = SpaceRenderer.newRenderer(2, 2, FormatRecommendation.COMPACT);
        assertEquals("  ", renderer.render());
        assertEquals("--", renderer.childRenderer(DifferenceType.REMOVAL).render());
    }

    @Test
    public void testCompactSpace() {
        assertEquals("", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.COMPACT).space());
    }

    @Test
    public void testCompactPrettyNewLine() {
        assertEquals("", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.COMPACT).prettyNewLine());
    }

    @Test
    public void testCompactMandatoryNewLine() {
        assertEquals("\n", SpaceRenderer.newRenderer(2, 0, FormatRecommendation.COMPACT).mandatoryNewLine());
    }

    @Test
    public void testCompactMandatoryCausesMoreSpacesOnRender() {
        final SpaceRenderer renderer = SpaceRenderer.newRenderer(2, 2, FormatRecommendation.COMPACT);
        assertEquals("  ", renderer.render());
        renderer.mandatoryNewLine();
        assertEquals("  ", renderer.render());

        final SpaceRenderer child = renderer.childRenderer(DifferenceType.UNMODIFIED);
        assertEquals("", child.render());
        child.mandatoryNewLine();
        assertEquals("  ", child.childRenderer(DifferenceType.UNMODIFIED).render());
    }
}