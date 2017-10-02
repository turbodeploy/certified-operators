package com.vmturbo.auth.api.authorization.spring;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.access.expression.method.MethodSecurityExpressionOperations;
import org.springframework.security.authentication.AuthenticationTrustResolver;
import org.springframework.security.authentication.AuthenticationTrustResolverImpl;
import org.springframework.security.core.Authentication;

/**
 * The SpringMethodSecurityExpressionHandler implements custom expression handler.
 */
public class SpringMethodSecurityExpressionHandler extends DefaultMethodSecurityExpressionHandler {
    /**
     * Creates the root object for expression evaluation.
     */
    protected MethodSecurityExpressionOperations createSecurityExpressionRoot(
            Authentication authentication, MethodInvocation invocation) {
        SpringMethodSecurityExpressionRoot root = new SpringMethodSecurityExpressionRoot(
                authentication);
        root.setThis(invocation.getThis());
        return root;
    }
}
