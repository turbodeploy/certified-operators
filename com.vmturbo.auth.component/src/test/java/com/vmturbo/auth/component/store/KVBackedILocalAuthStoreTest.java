package com.vmturbo.auth.component.store;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.vmturbo.auth.api.authentication.AuthenticationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * The KVBackedILocalAuthStoreTest tests the KV-backed Auth store.
 */
public class KVBackedILocalAuthStoreTest {

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
        boolean result2 = store.setRoles(AuthUserDTO.PROVIDER.LOCAL, "user0",
                                         ImmutableList.of("ADMIN2", "USER2"), ImmutableList.of(2L));
        Assert.assertTrue(result2);
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
}
