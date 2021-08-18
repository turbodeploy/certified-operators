package com.vmturbo.auth.component.store.sso;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OPERATIONAL_OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PREDEFINED_SECURITY_GROUPS_SET;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SHARED_OBSERVER;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;

/**
 * Test for {@link SsoUtil}.
 */
@RunWith(Parameterized.class)
public class SsoUtilTest {

    private static final String GROUP_1 = "group1";
    private final String groupName;

    /**
     * Constructor for parameters test.
     *
     * @param groupName group name
     */
    public SsoUtilTest(String groupName) {
        this.groupName = groupName;
    }

    /**
     * Parameters setup.
     *
     * @return list of parameters.
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{GROUP_1.toLowerCase()}, {GROUP_1.toUpperCase()}});
    }

    /**
     * Verify both lower and upper case group can be authorized.
     */
    @Test
    public void authenticateUserInGroup() {
        SsoUtil ssoUtil = new SsoUtil();
        SecurityGroupDTO securityGroup = new SecurityGroupDTO("", "", "");
        ssoUtil.putSecurityGroup(groupName, securityGroup);
        assertEquals(securityGroup, ssoUtil.authorizeSAMLUserInGroup("", groupName).get());
    }

    /**
     * Verify both lower and upper case group can be authorized with multiple external groups. It
     * only verified passing in single external groups.
     */
    @Test
    public void authenticateUserInGroupsBase() {
        SsoUtil ssoUtil = new SsoUtil();
        SecurityGroupDTO securityGroup = new SecurityGroupDTO("", "", "");
        ssoUtil.putSecurityGroup(groupName, securityGroup);
        assertEquals(securityGroup,
                ssoUtil.authorizeSAMLUserInGroups("user", ImmutableList.of(groupName),
                        false).get());
    }

    /**
     * Verify both lower and upper case group can be authorized for multiple external groups. It
     * verified passing in multiple external groups
     */
    @Test
    public void authenticateUserInGroupsExtended() {
        final SsoUtil ssoUtil = new SsoUtil();

        PREDEFINED_SECURITY_GROUPS_SET.forEach(
                group -> ssoUtil.putSecurityGroup(group.getDisplayName(), group));

        // not used
        SecurityGroupDTO securityGroup = new SecurityGroupDTO("", "", "");
        ssoUtil.putSecurityGroup(groupName, securityGroup);
        assertEquals(OPERATIONAL_OBSERVER, ssoUtil.authorizeSAMLUserInGroups("user",
                PREDEFINED_SECURITY_GROUPS_SET.stream()
                        .map(group -> group.getDisplayName())
                        .collect(Collectors.toList()), false).get().getRoleName());
    }

    /**
     * Verify role will NOT be assigned when no matched group found.
     */
    @Test
    public void authenticateUserInGroupsExtendedNegative() {
        final SsoUtil ssoUtil = new SsoUtil();

        PREDEFINED_SECURITY_GROUPS_SET.forEach(
                group -> ssoUtil.putSecurityGroup(group.getDisplayName(), group));

        // not used
        SecurityGroupDTO securityGroup = new SecurityGroupDTO("", "", "");
        ssoUtil.putSecurityGroup(groupName, securityGroup);
        assertEquals(Optional.empty(),
                ssoUtil.authorizeSAMLUserInGroups("user", ImmutableList.of("non-existedGroups"),
                        false));
    }

    /**
     * Verify when multiple external group flag is enabled, all the scopes are combined with SAMl SSO.
     */
    @Test
    public void authenticateUserInMultipleSAMLGroups() {
        SsoUtil ssoUtil = new SsoUtil();
        SecurityGroupDTO securityGroup0 = new SecurityGroupDTO("SHARED_GROUP0", "", SHARED_OBSERVER,
                Lists.newArrayList(1L, 3L));
        SecurityGroupDTO securityGroup1 = new SecurityGroupDTO("OBSERVER_GROUP1", "", OBSERVER,
                Lists.newArrayList(2L));
        SecurityGroupDTO securityGroup3 = new SecurityGroupDTO("OBSERVER_GROUP3", "", OBSERVER,
                Lists.newArrayList(4L));
        SecurityGroupDTO securityGroup4 = new SecurityGroupDTO("OPERATION_OBSERVER_GROUP4", "",
                OPERATIONAL_OBSERVER, Lists.newArrayList(4L));

        ssoUtil.putSecurityGroup(securityGroup0.getDisplayName(), securityGroup0);
        ssoUtil.putSecurityGroup(securityGroup1.getDisplayName(), securityGroup1);
        ssoUtil.putSecurityGroup(securityGroup3.getDisplayName(), securityGroup3);
        ssoUtil.putSecurityGroup(securityGroup4.getDisplayName(), securityGroup4);
        assertEquals(Lists.newArrayList(1L, 2L, 3L, 4L), ssoUtil.authorizeSAMLUserInGroups("user",
                ImmutableList.of(securityGroup1.getDisplayName(), securityGroup0.getDisplayName(),
                        securityGroup3.getDisplayName(), securityGroup4.getDisplayName()), true)
                .get()
                .getScopeGroups());
    }

    /**
     * Verify AD String Search Filter Character encoding. It will be applied when Multi-AD group feature
     * flag is enabled.
     * RFC: https://tools.ietf.org/search/rfc2254#page-5
     * Character Hex Representation.
     * *        \2A
     * (        \28
     * )        \29
     * \        \5C
     * Nul      \00
     */
    @Test
    public void testEscapeSpecialChars() {
        // normal case without special characters, there should no change.
        assertEquals("CN=Zeng Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com",
                SsoUtil.escapeSpecialChars("CN=Zeng Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com"));

        // special characters should be escaped.
        assertEquals("CN=Hatton\\2A Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com",
                SsoUtil.escapeSpecialChars("CN=Hatton* Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com"));
        assertEquals("CN=Hatton\\28 Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com",
                SsoUtil.escapeSpecialChars("CN=Hatton( Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com"));
        assertEquals("CN=Hatton\\29 Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com",
                SsoUtil.escapeSpecialChars("CN=Hatton) Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com"));
        assertEquals("CN=Hatton\\5C, Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com",
                SsoUtil.escapeSpecialChars("CN=Hatton\\, Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com"));
        assertEquals("CN=Hatton\\00 Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com",
                SsoUtil.escapeSpecialChars("CN=Hatton\000 Gary,OU=Markham Office,OU=R&D,DC=turbonomic,DC=com"));
    }
}