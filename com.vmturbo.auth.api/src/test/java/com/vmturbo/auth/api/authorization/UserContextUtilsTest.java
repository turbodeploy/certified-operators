package com.vmturbo.auth.api.authorization;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import io.grpc.Context;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

/**
 *
 */
public class UserContextUtilsTest {

    @Test
    public void testGetCurrentUserNoSessionInfo() {
        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
        Optional<String> userId = UserContextUtils.getCurrentUserId();
        // user id should not be found
        Assert.assertFalse(userId.isPresent());
        // also, no roles should be found
        Assert.assertFalse(UserContextUtils.getCurrentUserRoles().isPresent());
        Assert.assertFalse(UserContextUtils.currentUserHasRole("TEST").isPresent());
    }

    @Test
    public void testGetCurrentUserGrpc() {
        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
        // set a test grpc context
        Context testContext = Context.current().withValue(SecurityConstant.USER_ID_CTX_KEY, "USER")
                    .withValue(SecurityConstant.USER_UUID_KEY, "1")
                    .withValue(SecurityConstant.USER_ROLES_KEY, ImmutableList.of("ADMIN", "TEST"));
        Context previous = testContext.attach();
        // verify the user id and user name matched
        Optional<String> userId = UserContextUtils.getCurrentUserId();
        Assert.assertTrue(userId.isPresent());
        Assert.assertEquals("1", userId.get());
        String userName = UserContextUtils.getCurrentUserName();
        Assert.assertEquals("USER", userName);
        // verify the roles can be found
        Optional<List<String>> roles = UserContextUtils.getCurrentUserRoles();
        Assert.assertTrue(roles.isPresent());
        Assert.assertTrue(roles.get().containsAll(ImmutableList.of("TEST", "ADMIN")));
        Assert.assertTrue(UserContextUtils.currentUserHasRole("TEST").get());
        Assert.assertTrue(UserContextUtils.currentUserHasRole("ADMIN").get());
        Assert.assertFalse(UserContextUtils.currentUserHasRole("FAKE_ROLE").get());
        // restore the previous context
        testContext.detach(previous);
    }

    @Test
    public void testGetCurrentUserSpring() {
        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null,
                        "admin",
                        "pass",
                        "10.10.10.10",
                        "11111",
                        "token",
                        ImmutableList.of("ADMIN", "TEST"),
                        null),
                "admin000",
                CollectionUtils.emptyCollection());
        // put the test auth info into the spring security context
        SecurityContextHolder.getContext().setAuthentication(auth);
        // verify we can find the user id
        Optional<String> userId = UserContextUtils.getCurrentUserId();
        Assert.assertTrue(userId.isPresent());
        Assert.assertEquals("11111", userId.get());
        // verify the roles can be found
        Optional<List<String>> roles = UserContextUtils.getCurrentUserRoles();
        String currentUserName = UserContextUtils.getCurrentUserName();
        Assert.assertEquals("admin", currentUserName);
        Assert.assertTrue(roles.isPresent());
        Assert.assertTrue(roles.get().containsAll(ImmutableList.of("TEST", "ADMIN")));
        Assert.assertTrue(UserContextUtils.currentUserHasRole("TEST").get());
        Assert.assertTrue(UserContextUtils.currentUserHasRole("ADMIN").get());
        Assert.assertFalse(UserContextUtils.currentUserHasRole("FAKE_ROLE").get());
        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
    }
}
