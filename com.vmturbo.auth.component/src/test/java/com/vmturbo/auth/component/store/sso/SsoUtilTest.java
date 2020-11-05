package com.vmturbo.auth.component.store.sso;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OPERATIONAL_OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.PREDEFINED_SECURITY_GROUPS_SET;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

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
                ssoUtil.authorizeSAMLUserInGroups("user", ImmutableList.of(groupName)).get());
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
                        .collect(Collectors.toList())).get().getRoleName());
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
                ssoUtil.authorizeSAMLUserInGroups("user", ImmutableList.of("non-existedGroups")));
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