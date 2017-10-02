package com.vmturbo.api.component.external.api.mapper;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;

public class LoginProviderMapperTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testAllProvidersHaveMappings() {
        for (PROVIDER provider : PROVIDER.values()) {
            final String apiStr = LoginProviderMapper.toApi(provider);
            Assert.assertEquals(provider, LoginProviderMapper.fromApi(apiStr));
        }
    }

    @Test
    public void testBadApiStr() {
        expectedException.expect(IllegalStateException.class);
        LoginProviderMapper.fromApi("blah");
    }
}
