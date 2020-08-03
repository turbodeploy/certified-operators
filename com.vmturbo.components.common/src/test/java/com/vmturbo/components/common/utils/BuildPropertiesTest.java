package com.vmturbo.components.common.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

/**
 * Unit tests for the {@link BuildProperties} utility.
 */
public class BuildPropertiesTest {

    /**
     * Test that the properties get loaded correctly from the git.properties file in
     * the classpath.
     */
    @Test
    public void testProperties() {
        BuildProperties buildProperties = BuildProperties.get();
        assertThat(buildProperties.getVersion(), is("MyVersion"));
        assertThat(buildProperties.getShortCommitId(), is("shortCommitHash"));
        assertThat(buildProperties.getBranch(), is("testBranch"));
        assertThat(buildProperties.getBuildTime(), is("123"));
        assertThat(buildProperties.getCommitId(), is("someCommitHash!"));
        assertThat(buildProperties.isDirty(), is(true));
    }

}
