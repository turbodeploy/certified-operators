package com.vmturbo.auth.api.authorization.jwt;

import static com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier.IP_ADDRESS_CLAIM;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import com.vmturbo.auth.api.JWTKeyCodec;
import com.vmturbo.auth.api.authorization.AuthorizationException;
import com.vmturbo.auth.api.authorization.IAuthorizationVerifier;
import com.vmturbo.auth.api.authentication.ICredentials;
import com.vmturbo.auth.api.authentication.credentials.CredentialsBuilder;
import com.vmturbo.auth.api.authorization.kvstore.AuthStore;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests basic Verifier functions.
 */
public class VerifierTest {
    private static final String IP_ADDRESS = "10.10.10.1";

    /**
     * Test corrupted encoded public key.
     * We expect two parts separated by '|'.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEncodedPublicKey() throws Exception {
        JWTKeyCodec.decodePublicKey("ABC");
    }

    /**
     * Test corrupted encoded private key.
     * We expect two parts separated by '|'.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEncodedPrivateKey() throws Exception {
        JWTKeyCodec.decodePrivateKey("ABC");
    }

    /**
     * Happy test.
     */
    @Test
    public void testBasicCreds() throws Exception {
        ICredentials creds =
                CredentialsBuilder.builder().withUser("user").withPassword("password").build();
        // We will fake the authenticator

        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);

        String compact = Jwts.builder()
                             .setSubject("subject")
                             .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of("ADMINISTRATOR"))
                             .setExpiration(dt)
                             .signWith(SignatureAlgorithm.ES256, signingKey)
                             .compressWith(CompressionCodecs.GZIP)
                             .compact();

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());
        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(JWTKeyCodec.decodePublicKey(pubKeyStr));
        verifier.verify(token, new ArrayList<>());
    }

    /**
     * Fail the signature test test.
     */
    @Test
    public void testBasicCredsFailSignature() throws Exception {
        ICredentials creds =
                CredentialsBuilder.builder().withUser("user").withPassword("password").build();
        // We will fake the authenticator

        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);

        String compact = Jwts.builder().setSubject("subject")
                             .setExpiration(dt)
                             .signWith(SignatureAlgorithm.ES256, signingKey)
                             .compact();

        // Replace one character of the body.
        // The compact form has three parts, separated by '.':
        // <header>.<body>.<signature over header and body>
        char[] chars = compact.toCharArray();
        int bodyStart = compact.indexOf('.') + 1;
        chars[bodyStart + 1] = (chars[bodyStart + 1] == 'A') ? 'B' : 'A';
        compact = new String(chars);

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());
        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(JWTKeyCodec.decodePublicKey(pubKeyStr));
        try {
            verifier.verify(token, new ArrayList<>());
            Assert.fail("SignatureException is expected.");
        } catch (AuthorizationException e) {
            Assert.assertEquals(e.getCause().getClass(), SignatureException.class);
        }
    }

    /**
     * Happy test with adding IP address to JWT token.
     */
    @Test
    public void testBasicCredsWithIpAddress() throws Exception {
        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);
        PublicKey publicKey = keyPair.getPublic();

        String compact = Jwts.builder().setSubject("subject")
                .setExpiration(dt)
                .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of("USER", "ADMINISTRATOR"))
                .claim(IP_ADDRESS_CLAIM, IP_ADDRESS) // add IP address
                .compressWith(CompressionCodecs.GZIP)
                .signWith(SignatureAlgorithm.ES256, signingKey)
                .compact();

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());
        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(JWTKeyCodec.decodePublicKey(pubKeyStr));
        Collection<String> roles = new ArrayList<>();
        roles.add("USER");
        roles.add("ADMINISTRATOR");
        AuthUserDTO authUserDTO = verifier.verify(token, new ArrayList<>(roles));

        Claims claims = Jwts.parser()
                .setAllowedClockSkewSeconds(60)
                .setSigningKey(publicKey)
                .parseClaimsJws(compact)
                .getBody();
        Assert.assertEquals(IP_ADDRESS, claims.get(IP_ADDRESS_CLAIM,String.class));
    }

    /**
     * Happy test request with component JWT token.
     */
    @Test
    public void tesAuthenticateWithComponentJWT() throws Exception {

        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);
        PublicKey publicKey = keyPair.getPublic();

        String compact = Jwts.builder().setSubject("subject")
                .setExpiration(dt)
                .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of("USER", "ADMINISTRATOR"))
                .claim(IP_ADDRESS_CLAIM, IP_ADDRESS) // add IP address
                .compressWith(CompressionCodecs.GZIP)
                .signWith(SignatureAlgorithm.ES256, signingKey)
                .compact();

        // mock
        IAuthStore authStore = Mockito.mock(AuthStore.class);

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());

        Mockito.when(authStore.retrievePublicKey(Mockito.anyString())).thenReturn(Optional.of(pubKeyStr));
        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(authStore);
        Collection<String> roles = new ArrayList<>();
        roles.add("USER");
        roles.add("ADMINISTRATOR");
        AuthUserDTO authUserDTO = verifier.verifyComponent(token, "api-1");

        Claims claims = Jwts.parser()
                .setAllowedClockSkewSeconds(60)
                .setSigningKey(publicKey)
                .parseClaimsJws(compact)
                .getBody();
        Assert.assertEquals(IP_ADDRESS, claims.get(IP_ADDRESS_CLAIM,String.class));
    }


    /**
     * Negative test request with component JWT token.
     */
    @Test(expected = io.jsonwebtoken.SignatureException.class)
    public void tesAuthenticateWithInvalidComponentJWT() throws Exception {

        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();

        KeyGenerator keyGenerator = new KeyGenerator().invoke();
        KeyPair keyPair = keyGenerator.getKeyPair();
        PublicKey publicKey = keyGenerator.getPublicKey();

        KeyGenerator keyGenerator1 = new KeyGenerator().invoke();
        PrivateKey signingKey1 = keyGenerator1.getSigningKey();
        String compact = Jwts.builder().setSubject("subject")
                .setExpiration(dt)
                .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of("USER", "ADMINISTRATOR"))
                .claim(IP_ADDRESS_CLAIM, IP_ADDRESS) // add IP address
                .compressWith(CompressionCodecs.GZIP)
                .signWith(SignatureAlgorithm.ES256, signingKey1) //sign private key 1
                .compact();

        // mock
        IAuthStore authStore = Mockito.mock(AuthStore.class);

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());

        // try to verify original public key
        Mockito.when(authStore.retrievePublicKey(Mockito.anyString())).thenReturn(Optional.of(pubKeyStr));
        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(authStore);
        Collection<String> roles = new ArrayList<>();
        roles.add("USER");
        roles.add("ADMINISTRATOR");
        AuthUserDTO authUserDTO = verifier.verifyComponent(token, "api-1");
}

    /**
     * Happy test with roles match.
     */
    @Test
    public void testBasicCredsWithRoles() throws Exception {
        ICredentials creds =
                CredentialsBuilder.builder().withUser("user").withPassword("password").build();
        // We will fake the authenticator

        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);

        String compact = Jwts.builder().setSubject("subject")
                             .setExpiration(dt)
                             .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of("USER", "ADMINISTRATOR"))
                             .compressWith(CompressionCodecs.GZIP)
                             .signWith(SignatureAlgorithm.ES256, signingKey)
                             .compact();

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());
        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(JWTKeyCodec.decodePublicKey(pubKeyStr));
        Collection<String> roles = new ArrayList<>();
        roles.add("USER");
        roles.add("ADMINISTRATOR");
        verifier.verify(token, new ArrayList<>(roles));
    }

    /**
     * Happy test with roles match.
     */
    @Test
    public void testBasicCredsWithRolesMatchSingle() throws Exception {
        ICredentials creds =
                CredentialsBuilder.builder().withUser("user").withPassword("password").build();
        // We will fake the authenticator

        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);

        String compact = Jwts.builder().setSubject("subject")
                             .setExpiration(dt)
                             .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of("USER", "ADMINISTRATOR"))
                             .compressWith(CompressionCodecs.GZIP)
                             .signWith(SignatureAlgorithm.ES256, signingKey)
                             .compact();

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());
        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(JWTKeyCodec.decodePublicKey(pubKeyStr));
        Collection<String> roles = new ArrayList<>();
        roles.add("ADMINISTRATOR");
        verifier.verify(token, new ArrayList<>(roles));
    }

    /**
     * Failing test with roles mismatch.
     */
    @Test (expected = AuthorizationException.class)
    public void testBasicCredsWithRolesMismatchSingle() throws Exception {
        ICredentials creds =
                CredentialsBuilder.builder().withUser("user").withPassword("password").build();
        // We will fake the authenticator

        Date dt = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(dt);
        c.add(Calendar.DATE, 1);
        dt = c.getTime();

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);

        String compact = Jwts.builder().setSubject("subject")
                             .setExpiration(dt)
                             .claim(IAuthorizationVerifier.ROLE_CLAIM, "ADMINISTRATOR")
                             .compressWith(CompressionCodecs.GZIP)
                             .signWith(SignatureAlgorithm.ES256, signingKey)
                             .compact();

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());
        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(JWTKeyCodec.decodePublicKey(pubKeyStr));

        // We are going to fail, since we expected roles that were not assigned to us as part of the claim.
        Collection<String> roles = new ArrayList<>();
        roles.add("USER");
        roles.add("ADMINISTRATOR");
        verifier.verify(token, new ArrayList<>(roles));
    }

    /**
     * Fail the expiration test.
     */
    @Test(expected = AuthorizationException.class)
    public void testBasicCredsExpired() throws Exception {
        ICredentials creds =
                CredentialsBuilder.builder().withUser("user").withPassword("password").build();
        // We will fake the authenticator

        // Use the time in the past
        Date dt = new Date(System.currentTimeMillis() - 120000L);

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);

        String compact = Jwts.builder().setSubject("subject")
                             .setExpiration(dt)
                             .claim(IAuthorizationVerifier.ROLE_CLAIM, "USER|ADMINISTRATOR")
                             .compressWith(CompressionCodecs.GZIP)
                             .signWith(SignatureAlgorithm.ES256, signingKey)
                             .compact();

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());

        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(JWTKeyCodec.decodePublicKey(pubKeyStr));
        verifier.verify(token, new ArrayList<>());
    }

    /**
     * Fail the expiration test for cached entry.
     */
    @Test
    public void testBasicCredsExpiredAfterCache() throws Exception {
        ICredentials creds =
                CredentialsBuilder.builder().withUser("user").withPassword("password").build();
        // We will fake the authenticator

        // Use the time in the future. Not too long, as we need to wait for it to expire.
        long past = System.currentTimeMillis() - 57000L;
        Date dt = new Date(past);

        KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
        PrivateKey signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);

        String compact = Jwts.builder().setSubject("subject")
                             .setExpiration(dt)
                             .claim(IAuthorizationVerifier.ROLE_CLAIM, ImmutableList.of("USER", "ADMINISTRATOR"))
                             .compressWith(CompressionCodecs.GZIP)
                             .signWith(SignatureAlgorithm.ES256, signingKey)
                             .compact();

        // Encode the public key.
        String pubKeyStr = JWTKeyCodec.encodePublicKey(keyPair.getPublic());

        JWTAuthorizationToken token = new JWTAuthorizationToken(compact);
        JWTAuthorizationVerifier verifier =
                new JWTAuthorizationVerifier(JWTKeyCodec.decodePublicKey(pubKeyStr));

        // After first verify, we should have the entry in cache
        verifier.verify(token, new ArrayList<>());
        Assert.assertEquals(1, verifier.tokensCache_.size());

        // After second, we should have the token in cache, and it will be removed due to
        // expiration.
        long timeToWait = Math.min(3000L, 60100L - System.currentTimeMillis() + past);
        Thread.sleep(timeToWait);
        try {
            verifier.verify(token, new ArrayList<>());
            Assert.fail("Expecting AuthenticationException.");
        } catch (AuthorizationException e) {
        }
        Assert.assertTrue(verifier.tokensCache_.isEmpty());
    }

    private class KeyGenerator {
        private KeyPair keyPair;
        private PrivateKey signingKey;
        private PublicKey publicKey;

        public KeyPair getKeyPair() {
            return keyPair;
        }

        public PrivateKey getSigningKey() {
            return signingKey;
        }

        public PublicKey getPublicKey() {
            return publicKey;
        }

        public KeyGenerator invoke() {
            keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
            String privateKeyEncoded = JWTKeyCodec.encodePrivateKey(keyPair.getPrivate());
            signingKey = JWTKeyCodec.decodePrivateKey(privateKeyEncoded);
            publicKey = keyPair.getPublic();
            return this;
        }
    }
}
