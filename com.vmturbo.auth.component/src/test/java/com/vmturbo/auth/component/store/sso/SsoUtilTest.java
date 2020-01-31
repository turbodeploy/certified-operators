package com.vmturbo.auth.component.store.sso;

import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

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
        return Arrays.asList(new Object[][]{
                {GROUP_1.toLowerCase()}, {GROUP_1.toUpperCase()}
        });
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
}