package com.vmturbo.auth.component.policy;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.REPORT_EDITOR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.component.policy.UserPolicy.LoginPolicy;

/**
 * Validate {@link UserPolicy}.
 */
public class UserPolicyTest {
    private UserPolicy oneEditorPolicy;
    private UserPolicy twoEditorsPolicy;

    private final AuthUserDTO
            localUser1AdminAndEditorRole = new AuthUserDTO(PROVIDER.LOCAL, "user1", "uuid",
            ImmutableList.of(ADMINISTRATOR, REPORT_EDITOR));
    private final AuthUserDTO
            localUser1AdvisorAndEditorRole = new AuthUserDTO(PROVIDER.LOCAL, "user1", "uuid",
            ImmutableList.of(ADVISOR, REPORT_EDITOR));

    private final AuthUserDTO
            localUser2AdminAndEditorRole = new AuthUserDTO(PROVIDER.LOCAL, "user2", "uuid",
            ImmutableList.of(ADMINISTRATOR, REPORT_EDITOR));
    private final AuthUserDTO
            externalUser3AdvisorAndEditorRole = new AuthUserDTO(PROVIDER.LDAP, "user3", "uuid",
            ImmutableList.of(ADVISOR, REPORT_EDITOR));
    private final AuthUserDTO
            externalUser3AdminAndEditorRole = new AuthUserDTO(PROVIDER.LDAP, "user3", "uuid",
            ImmutableList.of(ADVISOR, REPORT_EDITOR));
    private final AuthUserDTO
            externalUser4AdvisorAndEditorRole = new AuthUserDTO(PROVIDER.LDAP, "user4", "uuid",
            ImmutableList.of(ADVISOR, REPORT_EDITOR));

    /**
     * Common code ran before every test.
     */
    @Before
    public void setup() {
        ReportPolicy oneEditor = mock(ReportPolicy.class);
        when(oneEditor.getAllowedMaximumEditors()).thenReturn(1);
        ReportPolicy twoEditor = mock(ReportPolicy.class);
        when(twoEditor.getAllowedMaximumEditors()).thenReturn(2);
        oneEditorPolicy = new UserPolicy(LoginPolicy.LOCAL_ONLY, oneEditor);
        twoEditorsPolicy = new UserPolicy(LoginPolicy.LOCAL_ONLY, twoEditor);
    }

    /**
     * LOCAL - Verify if the report policy allowed adding another report editor role, it will return true.
     * Otherwise, it will return false.
     */
    @Test
    public void testLocalUserPolicy() {
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(
                localUser1AdminAndEditorRole, Collections.emptyList())));
        assertFalse(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(
                localUser1AdminAndEditorRole, Collections.singletonList(
                        localUser2AdminAndEditorRole))));

        assertTrue(checkError(() -> twoEditorsPolicy.isAddingReportEditorRoleAllowed(
                localUser1AdminAndEditorRole, Collections.singletonList(
                        localUser2AdminAndEditorRole))));

        // Verify existing local user with editor role is allowed to update roles. See OM-66796.
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(
                localUser1AdvisorAndEditorRole, Collections.singletonList(
                        localUser1AdminAndEditorRole))));
    }

    /**
     * LDAP - Verify if the report policy allowed adding another report editor role, it will return true.
     * Otherwise, it will return false.
     */
    @Test
    public void testLdapUserPolicy() {
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(
                externalUser3AdvisorAndEditorRole, Collections.emptyList())));
        assertFalse(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(
                externalUser3AdvisorAndEditorRole, Collections.singletonList(
                        externalUser4AdvisorAndEditorRole))));

        assertTrue(checkError(() -> twoEditorsPolicy.isAddingReportEditorRoleAllowed(
                externalUser3AdvisorAndEditorRole, Collections.singletonList(
                        externalUser4AdvisorAndEditorRole))));
        // Verify existing external user with editor role is allowed to update roles. See OM-66796.
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(
                externalUser3AdvisorAndEditorRole, Collections.singletonList(
                        externalUser3AdminAndEditorRole))));
    }

    /**
     * Verify report policy enforce user limit regardless the local or external user.
     */
    @Test
    public void testMixedUserPolicy() {
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(
                localUser1AdminAndEditorRole, Collections.emptyList())));
        assertFalse(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(
                localUser1AdminAndEditorRole, Collections.singletonList(
                        externalUser3AdvisorAndEditorRole))));
    }

    /**
     * Test that a scoped user cannot have a REPORT_EDITOR role.
     */
    @Test
    public void testScopedUserDisallowedReportRole() {
        final AuthUserDTO scopedUser1 = new AuthUserDTO(PROVIDER.LOCAL, "", "uuid", "", "", "",
                ImmutableList.of(ADMINISTRATOR, REPORT_EDITOR), ImmutableList.of(123L));

        assertFalse(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(scopedUser1, Collections.emptyList())));
    }

    private boolean checkError(Runnable r) {
        try {
            r.run();
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
