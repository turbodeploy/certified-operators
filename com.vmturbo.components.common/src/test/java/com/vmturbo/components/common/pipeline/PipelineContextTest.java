package com.vmturbo.components.common.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineContextMemberException;
import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;

/**
 * Tests for {@link PipelineContext}.
 */
public class PipelineContextTest {

    /**
     * Expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final PipelineContext context = new TestPipelineContext();

    /**
     * testHasMember.
     */
    @Test
    public void testHasMember() {
        assertFalse(context.hasMember(Definitions.FOO));
        assertFalse(context.hasMember(Definitions.BAR));
        context.addMember(Definitions.FOO, "foo");
        assertTrue(context.hasMember(Definitions.FOO));
        assertFalse(context.hasMember(Definitions.BAR));
    }

    /**
     * testGetMember.
     */
    @Test
    public void testGetMember() {
        context.addMember(Definitions.FOO, "foo");
        assertEquals("foo", context.getMember(Definitions.FOO));
    }

    /**
     * testAddDuplicateMember.
     */
    @Test
    public void testAddDuplicateMember() {
        context.addMember(Definitions.FOO, "foo1");
        assertEquals("foo1", context.getMember(Definitions.FOO));
        context.addMember(Definitions.FOO, "foo2");
        assertEquals("foo2", context.getMember(Definitions.FOO));
    }

    /**
     * testAddNullMember.
     */
    @Test(expected = PipelineContextMemberException.class)
    public void testAddNullMember() {
        context.addMember(Definitions.FOO, null);
    }

    /**
     * testGetMemberNotPresent.
     */
    @Test(expected = PipelineContextMemberException.class)
    public void testGetMemberNotPresent() {
        context.getMember(Definitions.FOO);
    }

    /**
     * testDropMember.
     */
    @Test
    public void testDropMember() {
        assertFalse(context.hasMember(Definitions.FOO));
        context.addMember(Definitions.FOO, "foo");
        assertTrue(context.hasMember(Definitions.FOO));
        context.dropMember(Definitions.FOO);
        assertFalse(context.hasMember(Definitions.FOO));
    }

    /**
     * testDropMemberNotPresent.
     */
    @Test(expected = PipelineContextMemberException.class)
    public void testDropMemberNotPresent() {
        context.dropMember(Definitions.FOO);
    }

    /**
     * A test {@link PipelineContext}.
     */
    private static class TestPipelineContext extends PipelineContext {
        @Override
        public String getPipelineName() {
            return "test-pipeline";
        }
    }

    /**
     * TestMemberDefinitions.
     */
    private static class Definitions {
        private static final PipelineContextMemberDefinition<String> FOO
            = PipelineContextMemberDefinition.member(String.class, "foo", s -> s.length() + " characters");
        private static final PipelineContextMemberDefinition<Integer> BAR
            = PipelineContextMemberDefinition.member(Integer.class, "bar", i -> "one object");
    }
}