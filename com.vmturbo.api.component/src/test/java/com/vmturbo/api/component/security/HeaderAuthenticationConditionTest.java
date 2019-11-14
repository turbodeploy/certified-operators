package com.vmturbo.api.component.security;

import static com.vmturbo.api.component.security.HeaderAuthenticationCondition.ENABLED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;

/**
 * Verify {@link HeaderAuthenticationCondition}.
 */
public class HeaderAuthenticationConditionTest {

    /**
     * Verify the positive case, when the flag is passed from environment.
     */
    @Test
    public void testMatchesPositive() {
        ConditionContext context = mock(ConditionContext.class);
        Environment environment = mock(Environment.class);
        when(environment.getProperty(ENABLED)).thenReturn("true");
        when(context.getEnvironment()).thenReturn(environment);
        final HeaderAuthenticationCondition headerAuthenticationCondition =
                new HeaderAuthenticationCondition();
        assertTrue(headerAuthenticationCondition.matches(context, null));
    }

    /**
     * Verify the negative case, when the flag is NOT passed from environment.
     */
    @Test
    public void testMatchesNegative() {
        ConditionContext context = mock(ConditionContext.class);
        Environment environment = mock(Environment.class);
        when(environment.getProperty(ENABLED)).thenReturn("false");
        when(context.getEnvironment()).thenReturn(environment);
        final HeaderAuthenticationCondition headerAuthenticationCondition =
                new HeaderAuthenticationCondition();
        assertFalse(headerAuthenticationCondition.matches(context, null));

        when(environment.getProperty(ENABLED)).thenReturn("");
        assertFalse(headerAuthenticationCondition.matches(context, null));
    }
}