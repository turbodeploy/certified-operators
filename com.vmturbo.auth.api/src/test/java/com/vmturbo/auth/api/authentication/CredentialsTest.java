package com.vmturbo.auth.api.authentication;

import com.vmturbo.auth.api.authentication.ICredentials;
import com.vmturbo.auth.api.authentication.credentials.ADCredentials;
import com.vmturbo.auth.api.authentication.credentials.BasicCredentials;
import com.vmturbo.auth.api.authentication.credentials.CredentialsBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests credentials functions.
 */
public class CredentialsTest {
    @Test
    public void testHappyBasicCreds() throws Exception {
        ICredentials creds = CredentialsBuilder.builder().withUser("user").withPassword("password").build();
        Assert.assertTrue(creds instanceof BasicCredentials);
        Assert.assertEquals("user", ((BasicCredentials)creds).getUserName());
        Assert.assertEquals("password", ((BasicCredentials)creds).getPassword());
    }

    @Test
    public void testHappyADCreds() throws Exception {
        ICredentials creds = CredentialsBuilder.builder().withUser("user").withPassword("password").withDomain("domain").build();
        Assert.assertTrue(creds instanceof BasicCredentials);
        Assert.assertTrue(creds instanceof ADCredentials);
        Assert.assertEquals("user", ((ADCredentials)creds).getUserName());
        Assert.assertEquals("password", ((ADCredentials)creds).getPassword());
        Assert.assertEquals("domain", ((ADCredentials)creds).getDomain());
    }

    /**
     * Tests missing user name.
     */
    @Test (expected = NullPointerException.class)
    public void testMissingUser() throws Exception {
        ICredentials creds = CredentialsBuilder.builder().withPassword("password").build();
    }

    /**
     * Tests missing password.
     */
    @Test (expected = NullPointerException.class)
    public void testMissingPassword() throws Exception {
        ICredentials creds = CredentialsBuilder.builder().withUser("user").build();
    }

    /**
     * Tests missing user name.
     */
    @Test (expected = NullPointerException.class)
    public void testNullUser() throws Exception {
        ICredentials creds = CredentialsBuilder.builder().withUser(null).withPassword("password").build();
    }

    /**
     * Tests missing password.
     */
    @Test (expected = NullPointerException.class)
    public void testNullPassword() throws Exception {
        ICredentials creds = CredentialsBuilder.builder().withUser("user").withPassword(null).build();
    }

    /**
     * Tests missing user name.
     */
    @Test (expected = NullPointerException.class)
    public void testNullDomain() throws Exception {
        ICredentials creds = CredentialsBuilder.builder().withUser("user").withPassword("password").withDomain(null).build();
    }
}
