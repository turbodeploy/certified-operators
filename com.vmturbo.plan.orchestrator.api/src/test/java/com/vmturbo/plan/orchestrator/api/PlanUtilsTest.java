package com.vmturbo.plan.orchestrator.api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;

/**
 *
 */
public class PlanUtilsTest {

    @After
    public void teardown() {
        // clear the security context
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testCanCurrentUserAccessPlanAdmin() {
        // verify that an admin user can access a plan they did not create
        final PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(123)
                .setCreatedByUser("2")
                .setStatus(PlanStatus.READY)
                .build();

        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null, "admin", "pass", "10.10.10.10",
                        "1", "token", ImmutableList.of("Administrator"), null),
                "admin000",
                Collections.emptySet());
        SecurityContextHolder.getContext().setAuthentication(auth);
        assertTrue(PlanUtils.canCurrentUserAccessPlan(planInstance));
    }

    @Test
    public void testCanCurrentUserAccessPlanCreatorMatch() {
        // verify that a non-admin user can access a plan they created
        final PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(123)
                .setCreatedByUser("1")
                .setStatus(PlanStatus.READY)
                .build();

        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null, "nonadmin", "pass", "10.10.10.10",
                        "1", "token", ImmutableList.of("NOT_ADMIN"), null),
                "admin000",
                Collections.emptySet());
        SecurityContextHolder.getContext().setAuthentication(auth);
        assertTrue(PlanUtils.canCurrentUserAccessPlan(planInstance));
    }

    @Test
    public void testCanCurrentUserAccessPlanCreatorNotMatch() {
        // verify that a non-admin user cannot access a plan they did not create
        final PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(123)
                .setCreatedByUser("1")
                .setStatus(PlanStatus.READY)
                .build();

        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null, "nonadmin", "pass", "10.10.10.10",
                        "2", "token", ImmutableList.of("NOT_ADMIN"), null),
                "admin000",
                Collections.emptySet());
        SecurityContextHolder.getContext().setAuthentication(auth);
        assertFalse(PlanUtils.canCurrentUserAccessPlan(planInstance));
    }

    @Test
    public void testCanCurrentUserAccessPlanNonAdminAnonymousPlan() {
        // verify that a non-admin user can access a plan with no creator
        final PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(123)
                .setStatus(PlanStatus.READY)
                .build();

        Authentication auth = new UsernamePasswordAuthenticationToken(
                new AuthUserDTO(null, "nonadmin", "pass", "10.10.10.10",
                        "1", "token", ImmutableList.of("NOT_ADMIN"), null),
                "admin000",
                Collections.emptySet());
        SecurityContextHolder.getContext().setAuthentication(auth);
        assertTrue(PlanUtils.canCurrentUserAccessPlan(planInstance));
    }

    @Test
    public void testCanCurrentUserAccessPlanNoUser() {
        // verify that any plan can be accessed if there is no user in the calling context.
        final PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(123)
                .setCreatedByUser("1")
                .setStatus(PlanStatus.READY)
                .build();

        assertTrue(PlanUtils.canCurrentUserAccessPlan(planInstance));
    }
}
