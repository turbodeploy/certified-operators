package com.vmturbo.auth.component.store.sso;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OBSERVER;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.auth.api.authentication.AuthenticationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.auth.component.policy.UserPolicy;
import com.vmturbo.auth.component.policy.UserPolicy.LoginPolicy;
import com.vmturbo.auth.component.store.AuthProvider;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;

/**
 * Integration test for {@link SsoUtil}.
 */
@Ignore("only work when connection to vmturbo.com network.")
@RunWith(Parameterized.class)
public class SsoUtilIntegrationTest {

    private static final String PROVIDER_URI = "ldap://dell1.corp.vmturbo.com";
    /**
     * Username here should be specified WITHOUT domain. In the same time it DOES NOT MEAN that we
     * will not test username domain formats. Please, look at
     * com.vmturbo.auth.component.store.sso.SsoUtilIntegrationTest#data() method. It is taking value
     * provided here and builds tree cases to be checked in all tests:
     * 1. corp\USER_NAME_WITHOUT_DOMAIN
     * 2. USER_NAME_WITHOUT_DOMAIN@corp.vmturbo.com
     * 3. USER_NAME_WITHOUT_DOMAIN
     */
    private static final String USER_NAME_WITHOUT_DOMAIN = System.getProperty("username");
    private static final String PASSWORD = System.getProperty("password");
    private static final String WRONG_PASSWORD = "WRONGPASS";
    private static final String GROUP_NAME = "VPNusers";
    private static final String GROUP_NAME_ADDITION = "Dev";

    private static final String ADMIN_GROUP = "admin group";
    private static SsoUtil ssoUtil;
    /**
     * temp folder rule.
     */
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private Supplier<String> kvSupplier = () -> System.getProperty("com.vmturbo.kvdir");
    private GroupServiceMole groupService = spy(new GroupServiceMole());
    /**
     * grpc rule.
     */
    @Rule
    public GrpcTestServer mockServer = GrpcTestServer.newServer(groupService);

    private GroupServiceBlockingStub groupServiceClient;

    private final String username;

    /**
     * Constructor for parameters test.
     *
     * @param username group name
     */
    public SsoUtilIntegrationTest(@Nonnull String username) {
        this.username = username;
    }

    /**
     * Collection of data that will be used as a parameters for the test.
     *
     * @return parameters that will be used for test execution.
     */
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{"corp\\" + USER_NAME_WITHOUT_DOMAIN},
                        {USER_NAME_WITHOUT_DOMAIN},
                        {USER_NAME_WITHOUT_DOMAIN + "@corp.vmturbo.com"}});
    }

    /**
     * Before test.
     *
     * @throws IOException when failed on IO operations.
     */
    @Before
    public void setup() throws IOException {
        ssoUtil = new SsoUtil();
        ssoUtil.setDomainName("corp.vmturbo.com");
        ssoUtil.setLoginProviderURI(PROVIDER_URI);
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());
        groupServiceClient = GroupServiceGrpc.newBlockingStub(mockServer.getChannel());
    }

    /**
     * Test is AD available.
     */
    @Test
    public void testIsAdAvailable() {
        assertTrue(ssoUtil.isADAvailable());
    }

    /**
     * Authenticate user in group, positive case.
     */
    @Test
    public void testAuthenticateUserInGroup() {
        SecurityGroupDTO securityGroup = new SecurityGroupDTO(ADMIN_GROUP, "", ADMINISTRATOR);
        ssoUtil.putSecurityGroup(GROUP_NAME, securityGroup);
        assertNotNull(ssoUtil.authenticateUserInGroup(username, PASSWORD,
                        Collections.singleton(PROVIDER_URI), false));
    }

    /**
     * Authenticate user in multi groups with same role, result to all the groups.
     */
    @Test
    public void testAuthenticateUserInMultiGroupReturnMultipleGroups() {
        SecurityGroupDTO securityGroup =
                        new SecurityGroupDTO(ADMIN_GROUP, "", OBSERVER, Lists.newArrayList(1L));
        SecurityGroupDTO securityGroup1 =
                        new SecurityGroupDTO(ADMIN_GROUP, "", OBSERVER, Lists.newArrayList(2L));
        ssoUtil.putSecurityGroup(GROUP_NAME, securityGroup);
        ssoUtil.putSecurityGroup(GROUP_NAME_ADDITION, securityGroup1);
        final List<SecurityGroupDTO> securityGroupDTOList =
                        ssoUtil.authenticateUserInGroup(username, PASSWORD,
                                        Collections.singleton(PROVIDER_URI), true);
        assertNotNull(securityGroupDTOList);
        assertEquals(2, securityGroupDTOList.size());
        assertThat(securityGroupDTOList, hasItem(securityGroup));
        assertThat(securityGroupDTOList, hasItem(securityGroup1));
    }

    /**
     * Authenticate user in multi groups with different role, result to least privilege group at the
     * beginning.
     */
    @Test
    public void testAuthenticateUserInMultiDiffPrivilegeGroups() {
        SecurityGroupDTO securityGroup =
                        new SecurityGroupDTO(ADMIN_GROUP, "", OBSERVER, Lists.newArrayList(1L));
        SecurityGroupDTO securityGroup1 =
                        new SecurityGroupDTO(ADMIN_GROUP, "", ADVISOR, Lists.newArrayList(2L));
        ssoUtil.putSecurityGroup(GROUP_NAME, securityGroup);
        ssoUtil.putSecurityGroup(GROUP_NAME_ADDITION, securityGroup1);
        final List<SecurityGroupDTO> securityGroupDTOList =
                        ssoUtil.authenticateUserInGroup(username, PASSWORD,
                                        Collections.singleton(PROVIDER_URI), true);
        assertNotNull(securityGroupDTOList);
        assertEquals(2, securityGroupDTOList.size());
        assertEquals(securityGroupDTOList.get(0), securityGroup);
    }

    /**
     * Authenticate user in group, negative case - no such group.
     */
    @Test
    public void testAuthenticateUserInGroupNegative() {
        final List<SecurityGroupDTO> securityGroupDTOS =
                        ssoUtil.authenticateUserInGroup(username, PASSWORD,
                                        Collections.singleton(PROVIDER_URI), false);
        assertEquals(0, securityGroupDTOS.size());
    }

    /**
     * Authenticate with LDAP user, positive case.
     */
    @Test
    public void testAuthenticateUser() {
        SecurityGroupDTO securityGroup = new SecurityGroupDTO(ADMIN_GROUP, "", ADMINISTRATOR);
        ssoUtil.authenticateADUser(username, PASSWORD);
    }

    /**
     * Authenticate with LDAP user, negative case. Fill in with wrong username password
     * combination.
     */
    @Test(expected = SecurityException.class)
    public void testAuthenticateUserNegative() {
        SecurityGroupDTO securityGroup = new SecurityGroupDTO(ADMIN_GROUP, "", ADMINISTRATOR);
        ssoUtil.authenticateADUser(username, WRONG_PASSWORD);
    }

    /**
     * AD integration test, verify authentication passed with multi AD group enabled.
     *
     * @throws AuthenticationException when authentication failed.
     */
    @Test
    public void testADLoginWithmMultiGroups() throws AuthenticationException {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        final SsoUtil ssoUtil = new SsoUtil();
        ssoUtil.setDomainName("corp.vmturbo.com");
        ssoUtil.setLoginProviderURI(PROVIDER_URI);
        SecurityGroupDTO securityGroup = new SecurityGroupDTO(ADMIN_GROUP, "", OBSERVER,
                        com.google.common.collect.Lists.newArrayList(1L));
        SecurityGroupDTO securityGroup1 = new SecurityGroupDTO(ADMIN_GROUP, "", OBSERVER,
                        com.google.common.collect.Lists.newArrayList(2L));
        ssoUtil.putSecurityGroup(GROUP_NAME, securityGroup);
        ssoUtil.putSecurityGroup(GROUP_NAME_ADDITION, securityGroup1);
        AuthProvider store = new AuthProvider(keyValueStore, groupServiceClient, kvSupplier, null,
                        new UserPolicy(LoginPolicy.AD_ONLY), ssoUtil, true);

        final JWTAuthorizationToken authenticate =
                        store.authenticate(username, PASSWORD, "10.10.10.1");
        Assert.assertNotNull(authenticate);
    }
}
