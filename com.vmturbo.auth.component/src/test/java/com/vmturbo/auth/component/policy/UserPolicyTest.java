package com.vmturbo.auth.component.policy;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.REPORT_EDITOR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.component.policy.UserPolicy.LoginPolicy;

/**
 * Validate {@link UserPolicy}.
 */
public class UserPolicyTest {
    private final UserPolicy oneEditorPolicy = new UserPolicy(LoginPolicy.LOCAL_ONLY, new ReportPolicy(1));
    private final UserPolicy twoEditorsPolicy = new UserPolicy(LoginPolicy.LOCAL_ONLY, new ReportPolicy(2));

    private final AuthUserDTO localUser1 = new AuthUserDTO(PROVIDER.LOCAL, "", "uuid",
            ImmutableList.of(ADMINISTRATOR, REPORT_EDITOR));

    private final AuthUserDTO localUser2 = new AuthUserDTO(PROVIDER.LOCAL, "", "uuid",
            ImmutableList.of(ADMINISTRATOR, REPORT_EDITOR));
    private final AuthUserDTO ldapUser1 = new AuthUserDTO(PROVIDER.LDAP, "", "uuid",
            ImmutableList.of(ADVISOR, REPORT_EDITOR));
    private final AuthUserDTO ldapUser2 = new AuthUserDTO(PROVIDER.LDAP, "", "uuid",
            ImmutableList.of(ADVISOR, REPORT_EDITOR));

    /**
     * LOCAL - Verify if the report policy allowed adding another report editor role, it will return true.
     * Otherwise, it will return false.
     */
    @Test
    public void testLocalUserPolicy() {
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(localUser1, Collections.emptyList())));
        assertFalse(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(localUser1, Collections.singletonList(localUser2))));

        assertTrue(checkError(() -> twoEditorsPolicy.isAddingReportEditorRoleAllowed(localUser1, Collections.singletonList(localUser2))));
    }

    /**
     * LDAP - Verify if the report policy allowed adding another report editor role, it will return true.
     * Otherwise, it will return false.
     */
    @Test
    public void testLdapUserPolicy() {
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(ldapUser1, Collections.emptyList())));
        assertFalse(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(ldapUser1, Collections.singletonList(ldapUser2))));

        assertTrue(checkError(() -> twoEditorsPolicy.isAddingReportEditorRoleAllowed(ldapUser1, Collections.singletonList(ldapUser2))));
    }

    /**
     * Test that local and LDAP report policy enforcements are independent.
     */
    @Test
    public void testMixedUserPolicy() {
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(localUser1, Collections.emptyList())));
        assertTrue(checkError(() -> oneEditorPolicy.isAddingReportEditorRoleAllowed(localUser1, Collections.singletonList(ldapUser1))));
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