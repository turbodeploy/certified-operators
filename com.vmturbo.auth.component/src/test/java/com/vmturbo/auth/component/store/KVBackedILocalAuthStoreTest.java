package com.vmturbo.auth.component.store;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.vmturbo.auth.api.authentication.AuthenticationException;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;

/**
 * The KVBackedILocalAuthStoreTest tests the KV-backed Auth store.
 */
public class KVBackedILocalAuthStoreTest {

    // <inputUrl, secure, expectedUrl>: expected LDAP url based on user input url and secure flag
    private static final Table<String, Boolean, String> EXPECTED_LDAP_URL =
        new ImmutableTable.Builder<String, Boolean, String>()
            // user doesn't provide protocol or port number
            .put("ad.foo.com", true, "ldaps://ad.foo.com:636")
            .put("ad.foo.com", false, "ldap://ad.foo.com:389")

            // user provides full url
            .put("ldap://ad.foo.com:3268", true, "ldaps://ad.foo.com:3268")
            .put("ldap://ad.foo.com:3268", false, "ldap://ad.foo.com:3268")

            // user only provides protocol
            .put("ldaps://ad.foo.com", true, "ldaps://ad.foo.com:636")
            .put("ldap://ad.foo.com", false, "ldap://ad.foo.com:389")

            // user provides wrong protocol
            .put("ldap://ad.foo.com", true, "ldaps://ad.foo.com:636")
            .put("ldaps://ad.foo.com", false, "ldap://ad.foo.com:389")

            // user only provides default port number
            .put("ad.foo.com:636", true, "ldaps://ad.foo.com:636")
            .put("ad.foo.com:389", false, "ldap://ad.foo.com:389")

            // user only provides non-default port number
            .put("ad.foo.com:3268", true, "ldaps://ad.foo.com:3268")
            .put("ad.foo.com:3268", false, "ldap://ad.foo.com:3268")
            .build();

    /**
     * The KV prefix.
     */
    private static final String PREFIX = AuthProvider.PREFIX;

    /**
     * The JSON builder.
     */
    private static final Gson GSON = new GsonBuilder().create();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void init() throws Exception {
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());
    }

    @Test
    public void testAdminCheckInitColdStart() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        Assert.assertFalse(store.checkAdminInit());
    }

    @Test
    public void testAdminInitColdStart() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        Assert.assertTrue(store.initAdmin("admin", "password0"));
        Assert.assertTrue(store.checkAdminInit());
    }

    @Test
    public void testAdminInitWarmStart() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        Assert.assertTrue(store.initAdmin("admin", "password0"));
        Assert.assertTrue(store.checkAdminInit());
    }

    @Test
    public void testAdd() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(AuthUserDTO.PROVIDER.LOCAL, "user0", "password0",
                                   ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);
        Assert.assertTrue(result);

        Optional<String> jsonData = keyValueStore.get(PREFIX + AuthUserDTO.PROVIDER.LOCAL.name()
                                                      + "/USER0");
        Assert.assertTrue(jsonData.isPresent());

        // We store the keys in upper case.
        Assert.assertFalse(keyValueStore.get(PREFIX + "user0").isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertTrue(CryptoFacility.checkSecureHash(info.passwordHash, "password0"));
        Assert.assertEquals(ImmutableList.of("ADMIN", "USER"), info.roles);
        Assert.assertEquals(ImmutableList.of(1L), info.scopeGroups);
    }

    @Test
    public void testAuthenticate() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(AuthUserDTO.PROVIDER.LOCAL, "user0", "password0",
                                   ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);

        Optional<String> jsonData = keyValueStore.get(PREFIX + AuthUserDTO.PROVIDER.LOCAL.name()
                                                      + "/USER0");
        Assert.assertTrue(jsonData.isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertTrue(CryptoFacility.checkSecureHash(info.passwordHash, "password0"));
        Assert.assertEquals(ImmutableList.of("ADMIN", "USER"), info.roles);
        Assert.assertEquals(ImmutableList.of(1L), info.scopeGroups);

        Assert.assertNotNull(store.authenticate("user0", "password0"));
    }


    @Test
    public void testAuthenticateWithIpAddress() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(AuthUserDTO.PROVIDER.LOCAL, "user1", "password0",
                ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);

        Optional<String> jsonData = keyValueStore.get(PREFIX + AuthUserDTO.PROVIDER.LOCAL.name()
                + "/USER1");
        Assert.assertTrue(jsonData.isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertTrue(CryptoFacility.checkSecureHash(info.passwordHash, "password0"));
        Assert.assertEquals(ImmutableList.of("ADMIN", "USER"), info.roles);
        Assert.assertEquals(ImmutableList.of(1L), info.scopeGroups);

        Assert.assertNotNull(store.authenticate("user1", "password0", "10.10.10.1"));
    }

    // Happy path
    @Test
    public void testAuthorizationWithExternalUser() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(PROVIDER.LDAP, "user1", "password0",
                ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);

        Optional<String> jsonData = keyValueStore.get(PREFIX + PROVIDER.LDAP.name()
                + "/USER1");
        Assert.assertTrue(jsonData.isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertEquals(ImmutableList.of("ADMIN", "USER"), info.roles);
        Assert.assertEquals(ImmutableList.of(1L), info.scopeGroups);

        Assert.assertNotNull(store.authorize("user1", "10.10.10.1"));
    }

    @Test(expected = AuthorizationException.class)
    public void testAuthorizationWithInvalidExternalUser() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(PROVIDER.LDAP, "user1", "password0",
                ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);

        Optional<String> jsonData = keyValueStore.get(PREFIX + PROVIDER.LDAP.name()
                + "/USER1");
        Assert.assertTrue(jsonData.isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertEquals(ImmutableList.of("ADMIN", "USER"), info.roles);
        Assert.assertEquals(ImmutableList.of(1L), info.scopeGroups);

        // user2 is not the valid external user
        Assert.assertNotNull(store.authorize("user2", "10.10.10.1"));
    }


    @Test
    public void testAuthorizationWithExternalGroup() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        SecurityGroupDTO securityGroupDTO = new SecurityGroupDTO("group",
                "group",
                "administrator", ImmutableList.of(1L));
        store.createSecurityGroup(securityGroupDTO);
        Assert.assertNotNull(store.authorize("user1", "group", "10.10.10.1"));
    }

    @Test(expected = AuthorizationException.class)
    public void testAuthorizationWithInvalidExternalGroup() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        SecurityGroupDTO securityGroupDTO = new SecurityGroupDTO("group",
                "group",
                "administrator");
        store.createSecurityGroup(securityGroupDTO);
        // group1 is not a valid group
        Assert.assertNotNull(store.authorize("user1", "group1", "10.10.10.1"));
    }

    @Test
    public void testModifyPassword() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(AuthUserDTO.PROVIDER.LOCAL, "user0", "password0",
                                   ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);
        Assert.assertTrue(result);
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        grantedAuths.add(new SimpleGrantedAuthority("ROLE_NONADMINISTRATOR"));
        // The password is hidden.
        Authentication authentication = new UsernamePasswordAuthenticationToken("user0",
                                                                                "***",
                                                                                grantedAuths);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        boolean result2 = store.setPassword("user0", "password0", "password1");
        Assert.assertTrue(result2);
        try {
            store.authenticate("user0", "password0");
            Assert.fail();
        } catch (AuthenticationException e) {
        }
        Assert.assertNotNull(store.authenticate("user0", "password1"));

        Optional<String> jsonData = keyValueStore.get(PREFIX + AuthUserDTO.PROVIDER.LOCAL.name()
                                                      + "/USER0");
        Assert.assertTrue(jsonData.isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertTrue(CryptoFacility.checkSecureHash(info.passwordHash, "password1"));
        Assert.assertEquals(ImmutableList.of("ADMIN", "USER"), info.roles);
        Assert.assertEquals(ImmutableList.of(1L), info.scopeGroups);
    }

    @Test
    public void testModifyRoles() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(AuthUserDTO.PROVIDER.LOCAL, "user0", "password0",
                                   ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);
        ResponseEntity<String> result2 = store.setRoles(AuthUserDTO.PROVIDER.LOCAL, "user0",
            ImmutableList.of("ADMIN2", "USER2"), ImmutableList.of(2L));
        Assert.assertEquals(HttpStatus.OK, result2.getStatusCode());
        Assert.assertNotNull(store.authenticate("user0", "password0"));

        Optional<String> jsonData = keyValueStore.get(PREFIX + AuthUserDTO.PROVIDER.LOCAL.name()
                                                      + "/USER0");
        Assert.assertTrue(jsonData.isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertTrue(CryptoFacility.checkSecureHash(info.passwordHash, "password0"));
        Assert.assertEquals(ImmutableList.of("ADMIN2", "USER2"), info.roles);
        Assert.assertEquals(ImmutableList.of(2L), info.scopeGroups);
    }

    @Test
    public void testRemovePresent() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(AuthUserDTO.PROVIDER.LOCAL, "user0", "password0",
                                   ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);
        Assert.assertTrue(store.remove("user0"));

        Optional<String> jsonData = keyValueStore.get(PREFIX + "USER0");
        Assert.assertFalse(jsonData.isPresent());
    }

    @Test
    public void testRemoveAbsent() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        Assert.assertFalse(store.remove("user0"));
        Optional<String> jsonData = keyValueStore.get(PREFIX + AuthUserDTO.PROVIDER.LOCAL.name()
                                                      + "/USER0");
        Assert.assertFalse(jsonData.isPresent());
    }

    @Test
    public void testLock() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(AuthUserDTO.PROVIDER.LOCAL, "user0", "password0",
                                   ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);

        Optional<String> jsonData = keyValueStore.get(PREFIX + AuthUserDTO.PROVIDER.LOCAL.name()
                                                      + "/USER0");
        Assert.assertTrue(jsonData.isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertTrue(CryptoFacility.checkSecureHash(info.passwordHash, "password0"));
        Assert.assertEquals(ImmutableList.of("ADMIN", "USER"), info.roles);

        Assert.assertNotNull(store.authenticate("user0", "password0"));
        store.lock(new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user0", null, null, null, null,
                                   ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L)));

        exception.expect(AuthenticationException.class);
        store.authenticate("user0", "password0");
    }

    @Test
    public void testUnlock() throws Exception {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        boolean result = store.add(AuthUserDTO.PROVIDER.LOCAL, "user0", "password0",
                                   ImmutableList.of("ADMIN", "USER"), ImmutableList.of(1L));
        Assert.assertTrue(result);

        Optional<String> jsonData = keyValueStore.get(PREFIX + AuthUserDTO.PROVIDER.LOCAL.name()
                                                      + "/USER0");
        Assert.assertTrue(jsonData.isPresent());

        AuthProvider.UserInfo
                info = GSON.fromJson(jsonData.get(), AuthProvider.UserInfo.class);

        Assert.assertTrue(CryptoFacility.checkSecureHash(info.passwordHash, "password0"));
        Assert.assertEquals(ImmutableList.of("ADMIN", "USER"), info.roles);

        Assert.assertNotNull(store.authenticate("user0", "password0"));
        store.lock(new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user0", null,
                                   ImmutableList.of("ADMIN", "USER")));
        // We use try/catch so that we can test the fact we cat unlock successfully.
        try {
            store.authenticate("user0", "password0");
            Assert.fail();
        } catch (AuthenticationException e) {
        }

        store.unlock(new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user0", null,
                                     ImmutableList.of("ADMIN", "USER")));
        Assert.assertNotNull(store.authenticate("user0", "password0"));
    }

    @Test
    public void testLDAPInputUrlWithProtocolAndPortNumber() {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);

        EXPECTED_LDAP_URL.cellSet().forEach(cell ->
            Assert.assertEquals(
                "Expecting: " + cell.getValue() + " for given url: "+ cell.getRowKey() +
                    " and secure flag: " + cell.getColumnKey(),
                cell.getValue(),
                store.createLoginProviderURI(cell.getRowKey(), cell.getColumnKey()))
        );
    }

    @Test
    public void testCreateSecurityGroup() {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        SecurityGroupDTO securityGroupDTO = new SecurityGroupDTO("group1",
            "DedicatedCustomer",
            "administrator");
        store.createSecurityGroup(securityGroupDTO);

        Assert.assertEquals(1, store.getSecurityGroups().size());
        final SecurityGroupDTO g = store.getSecurityGroups().get(0);
        Assert.assertEquals("group1", g.getDisplayName());
        Assert.assertEquals("DedicatedCustomer", g.getType());
        Assert.assertEquals("administrator", g.getRoleName());
    }

    @Test(expected = SecurityException.class)
    public void testCreateSecurityGroupWhichAlreadyExists() {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        SecurityGroupDTO securityGroupDTO = new SecurityGroupDTO("group1",
            "DedicatedCustomer",
            "administrator");
        store.createSecurityGroup(securityGroupDTO);
        // create new security group with same name
        store.createSecurityGroup(securityGroupDTO);
    }

    @Test
    public void testUpdateSecurityGroup() {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        SecurityGroupDTO securityGroupDTO = new SecurityGroupDTO("group1",
            "DedicatedCustomer",
            "administrator");
        store.createSecurityGroup(securityGroupDTO);

        // update existing group
        SecurityGroupDTO newSecurityGroupDTO = new SecurityGroupDTO("group1",
            "SharedCustomer",
            "observer",
            Lists.newArrayList(11L));
        store.updateSecurityGroup(newSecurityGroupDTO);

        Assert.assertEquals(1, store.getSecurityGroups().size());
        final SecurityGroupDTO g = store.getSecurityGroups().get(0);
        Assert.assertEquals("group1", g.getDisplayName());
        Assert.assertEquals("SharedCustomer", g.getType());
        Assert.assertEquals("observer", g.getRoleName());
        Assert.assertEquals(Sets.newHashSet(11L), Sets.newHashSet(g.getScopeGroups()));
    }

    @Test(expected = SecurityException.class)
    public void testUpdateSecurityGroupWhichDoesNotExist() {
        KeyValueStore keyValueStore = new MapKeyValueStore();
        AuthProvider store = new AuthProvider(keyValueStore);
        SecurityGroupDTO securityGroupDTO = new SecurityGroupDTO("group1",
            "DedicatedCustomer",
            "administrator");
        store.createSecurityGroup(securityGroupDTO);

        // update existing group
        SecurityGroupDTO newSecurityGroupDTO = new SecurityGroupDTO("group2",
            "SharedCustomer",
            "observer",
            Lists.newArrayList(11L));
        store.updateSecurityGroup(newSecurityGroupDTO);
    }
}
