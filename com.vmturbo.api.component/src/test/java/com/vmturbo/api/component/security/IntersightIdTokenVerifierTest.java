package com.vmturbo.api.component.security;

import static com.vmturbo.api.component.security.IntersightIdTokenVerifier.INTERSIGHT;
import static org.springframework.test.util.AssertionErrors.assertEquals;

import java.security.Key;
import java.security.PublicKey;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.authentication.AuthenticationException;

/**
 * Test for {@link IntersightIdTokenVerifier}.
 */
public class IntersightIdTokenVerifierTest {

    /**
     * RSA public key provided by Cisco with prefix and suffix.
     */
    public static final String PUBLIC_KEY_WITH_PREFIX_SUFFIX =
            "-----BEGIN RSA PUBLIC KEY----- MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA10XjWtx/V6pXPUh0RK+b QycdKJlRlTxzPkyUXPTTD5Q7aUdNpNHki8H3dv643dDuH133ABU0BC4Zh/TjqfdU Op7Bd72VrE/HGStqcG1pxydYbUaSC3B9uAQAEAyuWuT38aCOrPjTiSmNCdo9ZGT9 Y70GjXcUbuXKo05UtzqC2vrQGcEQu2+0N+ngpaMT9yFExNuwOvNNM4BEEL1VR35n vOVjkmjmFpvKHkzZWABJ8aoK5E3b1ABcDwvyHdnk4lP0DjqsW9XjciMZ+Ov6fUn3 ltpt4AFBvD4korkzc1qF7WcArrXUmn6xvpw78rcyMVI23i2SMLBTXk+efGbSmtqC 19VqtbbxfVdLftcNNUM2I+5QoiAp8RNlDqWh2gUq8335sLLjsEL+sbMcMLFtml0b Ym8//ufQzyMZph43To1PIMyuDfxUXFa97DJBsHE5T4e4k3yb9mejYpDDL3iOGs1f 3/YRVmRBrSGjEXcyqo1lttm5mbunlo5018qO4TnlyKGeanE3wuuHJ6iWVv36QQgz qDqN0xOeixcdbRXX7RyKlAeMGrioOnF+sdIuk2Nh3xzVxE+cUTfZPDXvBUyoQP3J bY7x6uCr56d9UX6HAgQnW+ErkrrMLiU4Az8W8W2DwjSdanP7X6Jcq2PAIOQxRp27 v1kw5LWSaUk0x8bC2jfDclcCAwEAAQ== -----END RSA PUBLIC KEY-----";
    /**
     * EC public key provided by Cisco with prefix and subfix
     */
    public static final String PUBLIC_KEY_WITH_PREFIX_SUBFIX_ES256 =
            "-----BEGIN PUBLIC KEY-----MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAELnjJYfoFzXI7nQQPU8HtcDrUS50Azzs9xlTzbDQBFp4GUQjzbetxNLYDuph+QuOvufXM7AbFYMxkHMpEO6KNHw==-----END PUBLIC KEY-----";
    /**
     * RSA public key provided by Cisco without prefix and suffix.
     */
    public static final String PUBLIC_KEY_ONLY =
            "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA10XjWtx/V6pXPUh0RK+b QycdKJlRlTxzPkyUXPTTD5Q7aUdNpNHki8H3dv643dDuH133ABU0BC4Zh/TjqfdU Op7Bd72VrE/HGStqcG1pxydYbUaSC3B9uAQAEAyuWuT38aCOrPjTiSmNCdo9ZGT9 Y70GjXcUbuXKo05UtzqC2vrQGcEQu2+0N+ngpaMT9yFExNuwOvNNM4BEEL1VR35n vOVjkmjmFpvKHkzZWABJ8aoK5E3b1ABcDwvyHdnk4lP0DjqsW9XjciMZ+Ov6fUn3 ltpt4AFBvD4korkzc1qF7WcArrXUmn6xvpw78rcyMVI23i2SMLBTXk+efGbSmtqC 19VqtbbxfVdLftcNNUM2I+5QoiAp8RNlDqWh2gUq8335sLLjsEL+sbMcMLFtml0b Ym8//ufQzyMZph43To1PIMyuDfxUXFa97DJBsHE5T4e4k3yb9mejYpDDL3iOGs1f 3/YRVmRBrSGjEXcyqo1lttm5mbunlo5018qO4TnlyKGeanE3wuuHJ6iWVv36QQgz qDqN0xOeixcdbRXX7RyKlAeMGrioOnF+sdIuk2Nh3xzVxE+cUTfZPDXvBUyoQP3J bY7x6uCr56d9UX6HAgQnW+ErkrrMLiU4Az8W8W2DwjSdanP7X6Jcq2PAIOQxRp27 v1kw5LWSaUk0x8bC2jfDclcCAwEAAQ==";
    /**
     * EC public key provided by Cisco without prefix and subfix
     */
    public static final String PUBLIC_KEY_ES256 =
            "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAELnjJYfoFzXI7nQQPU8HtcDrUS50A zzs9xlTzbDQBFp4GUQjzbet  xNLYDuph+QuOvufXM7AbFYMxkHMpEO6KNHw==";
    /**
     * Sample RSA JTW token provided by Cisco.
     */
    public static final String JWT_TOKEN =
            "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJBY2NvdW50IjoiNWRjMjI2YmM3NTY0NjEyZDMwNWU2MGYwIiwiQWNjb3VudE5hbWUiOiJzZXJ2aWNlLWFkbWluIiwiVXNlciI6IjVkYzIyYTJjNzU2NDYxMmQzMDVlNjg0OCIsIlJvbGVzIjoiU3lzdGVtIEFkbWluaXN0cmF0b3IiLCJSb2xlSWRzIjoiNWRjMjI2YmM3NTY0NjEyZDMwNWU2MTA4IiwiRXBEZXZpY2VUeXBlIjoiIiwiRXBQcml2aWxlZ2VzIjoiIiwiU2VydmljZUFkbWluIjoieWVzIiwiRG9tYWluR3JvdXAiOiI1ZGMyMjZiYzc1NjQ2MTJkMzA1ZTYwZjEiLCJEb21haW5Hcm91cFBhcnRLZXkiOiJINWRjMjI2YmM3NTY0NjEyZDMwNWU2MGYxIiwiUHJveHlJbnN0YW5jZVBhcnRpdGlvbiI6IiIsIlBlcm1pc3Npb25JZCI6IjVkYzIyNmJjNzU2NDYxMmQzMDVlNjEwYiIsIlBlcm1pc3Npb25OYW1lIjoiU3lzdGVtIEFkbWluaXN0cmF0b3IiLCJhdWQiOiJJTyxCYXJjZWxvbmEiLCJleHAiOjE1NzM2Mjc2MTksImlhdCI6MTU3MzYyNTgxOSwiaXNzIjoiYmFycmFjdWRhIiwic3ViIjoiZGV2b3BzLWFkbWluQGxvY2FsIn0.cwD-QJjHTDZy7km7d3yJ2EupZ-C-X5EoZRSuSwuox1Jjbs3yD00zdRFkYSyokeKE-6lFb50Lvc_ghN_MlLpuwnilUcihZS5CHlESI9MnZL8tyPLo0aQ3-_bteEoJPSOS8QANhL5oUY2MOnrupiaf90_CCqwR550Ixuk2f0u5-uAUWdxMUXpK18S8jmDp5KhDdCuwv7UvRr4KuiGIBGg3EmcCP_OotGmm9nPT6pFd4GSTVPW4PLgihip4LO1AcUM__W7VVQ4w3CXK6JT-nitLG7KHaQjbOr5uixs07mKJLRfzb8DugEstxs8HBq4gmuaeyYOuAVGrATiqKCkfcdrP6vgcA2Dk7_GlFYSuDJ8v7OCwo4NcPHTQ_Ib-YbizF_CRF3aLDi7b0B2f31s3dQFXeymehLxJpUc-dJa7We_0JjBySijQPsMTZW4kYHFyhp3pBEkcKiRhtG2D2EBvJdY-A2DDnj8fbaPvd27MFibY9_RwlVs9hL-X1RgvLhOhFwDxFoieQHLeRycJ0YpXvkFEsBM6DA3yZ_pIZFXIAr-bLKdFe50ksOfW1bMAxZ8Z2bRCyrzMR4ZMu51XAIvyMnE8W-jZSDs4L7So1Ddre0Hshc7VZ2gVOGE4JKnfRBJwOGTwxYlM9RuTd2bXl3F3JrUJIHyAaSwPdKo0kEtCKLN4Qfo";
    /**
     * Sample JTW EC token provided by Cisco.
     */
    public static final String JWT_ES256_TOKEN =
            "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbnRlcnNpZ2h0Ijp7ImFjY291bnRfaWQiOiI1ZGMyMjZiYzc1NjQ2MTJkMzA1ZTYxMmEiLCJhY2NvdW50X25hbWUiOiJhZG1pbiIsInVzZXJfaWQiOiI1ZGMyMmEyZjc1NjQ2MTJkMzA1ZTY4NTciLCJyb2xlcyI6IkFjY291bnQgQWRtaW5pc3RyYXRvciIsInJvbGVfaWRzIjoiNWRjMjI2YmM3NTY0NjEyZDMwNWU2MjAxIiwic2VydmljZV9hZG1pbiI6Im5vIiwiZG9tYWluZ3JvdXBfaWQiOiI1ZGMyMjZiYzc1NjQ2MTJkMzA1ZTYxMmIiLCJkb21haW5ncm91cF9wYXJ0aXRpb25fa2V5IjoiNWRjMjI2YmM3NTY0NjEyZDMwNWU2MTJiIiwicGVybWlzc2lvbl9pZCI6IjVkYzIyNmJjNzU2NDYxMmQzMDVlNjIwMyIsInBlcm1pc3Npb25fbmFtZSI6IkFjY291bnQgQWRtaW5pc3RyYXRvciIsImVuZm9yY2Vfb3JncyI6Im5vIiwicGVybWlzc2lvbl9yZXN0cmljdGVkX3RvX29yZ3MiOiJubyJ9LCJhdWQiOiJodHRwczovL3ZhY2h5dXRhLW9wLTIuY2lzY28uY29tIiwiZXhwIjoxNTc2NjU3MDQ2LCJpYXQiOjE1NzY2NTUyNDYsImlzcyI6Imh0dHBzOi8vdmFjaHl1dGEtb3AtMi5jaXNjby5jb20iLCJzdWIiOiJhZG1pbkBsb2NhbCJ9.hl0Twp8AKSweKV8ZBYKU0gI_Bos-GsLHM4Q9dit4HlB2lLANP3i6k4-dwvhM7H60xDTGWybzH8DkvzCzC-xNSw";
    private static final int CLOCK_SKEW_SECOND = 60 * 60 * 24 * 365 * 30; // 30 years
    private static final String KEY = "key";
    private static final String DEVICE_ADMINISTRATOR = "Device Administrator";
    private static final String WORKLOAD_OPTIMIZER_ADVISOR = "Workload Optimizer Advisor";
    private static final String WORKLOAD_OPTIMIZER_OBSERVER = "Workload Optimizer Observer";
    private static final String WORKLOAD_OPTIMIZER_DEPLOYER = "Workload Optimizer Deployer";
    private static final String WORKLOAD_OPTIMIZER_ADMINISTRATOR =
            "Workload Optimizer Administrator";
    private static final String WORKLOAD_OPTIMIZER_AUTOMATOR = "Workload Optimizer Automator";
    private static final String READ_ONLY = "Read-Only";
    private static final String SECRET_KEY =
            "oeRaYY7Wo24sDqKSX3IM9ASGmdGPmkTd9jo1QTy4b7P9Ze5_9hKolVX8xNrQDcNRfVEdTZNOuOyqEGhXEbdJI-ZQ19k_o9MI0y3eZN2lp9jow55FfXMiINEdt1XR85VipRLSOkT6kSpzs2x-jbLDiz9iFVzkd81YKxMgPA7VfZeQUm4n-mOmnWMaVX30zGFU4L3oPBctYKkl4dYfqYWqRNfrgPJVi5DGFjywgxx0ASEiJHtV72paI3fDR2XwlSkyhhmY-ICjCRmsJN4fX1pdoL8a18-aQrvyu4j0Os6dVPYIoPvvY0SAZtWYKHfM15g7A3HD4cVREf9cUsprCRK93w";
    private static final String PERMISSION_TAG = "roles";
    //The JWT signature algorithm we will be using to sign the token
    private static SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;
    private static long nowMillis = System.currentTimeMillis();
    private static Date now = new Date(nowMillis);
    //We will sign our JWT with our ApiKey secret
    private static byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary(SECRET_KEY);
    private static Key signingKey =
            new SecretKeySpec(apiKeySecretBytes, signatureAlgorithm.getJcaName());
    private IntersightIdTokenVerifier verifier;
    private HeaderMapper mapper =
            new IntersightHeaderMapper(Collections.emptyMap(), "", "", "", "");

    //Sample method to construct a JWT
    private static String createJWT(final ImmutableList<String> list) {
        //Let's set the JWT Claims
        final ImmutableMap<String, List<String>> value = ImmutableMap.of(PERMISSION_TAG, list);
        return buildJWT(value);
    }

    private static String createJWT(final String roles) {
        //Let's set the JWT Claims
        final ImmutableMap<String, String> value = ImmutableMap.of(PERMISSION_TAG, roles);
        return buildJWT(value);
    }

    private static String buildJWT(ImmutableMap<String, ?> value) {
        JwtBuilder builder = Jwts.builder()
                .setId("SOMEID1234")
                .setIssuedAt(now)
                .setSubject("admin@local")
                .setIssuer("test")
                .claim(INTERSIGHT, value)
                .signWith(signatureAlgorithm, signingKey);

        long expMillis = nowMillis + 800000;
        Date exp = new Date(expMillis);
        builder.setExpiration(exp);

        //Builds the JWT and serializes it to a compact, URL-safe string
        return builder.compact();
    }

    /**
     * Before every test.
     */
    @Before
    public void setup() {
        verifier = new IntersightIdTokenVerifier(PERMISSION_TAG);
    }

    /**
     * Verify previous version JWT token with public key have prefix and suffix.
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testVerifyWithPrefixAndSuffix() throws AuthenticationException {
        Optional<PublicKey> jwtPublicKey =
                mapper.buildPublicKey(Optional.of(PUBLIC_KEY_WITH_PREFIX_SUFFIX));
        Optional<String> jwtToken = Optional.of(JWT_TOKEN);
        Pair<String, String> pair = verifier.verify(jwtPublicKey, jwtToken, CLOCK_SKEW_SECOND);
        assertEquals("Subject should be devops-admin@local", "devops-admin@local", pair.first);
        assertEquals("Role should be System Administrator", "System Administrator", pair.second);
    }

    /**
     * Verify previous version JWT token with public key that doesn't have prefix and suffix.
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testVerify() throws AuthenticationException {
        Optional<PublicKey> jwtPublicKey = mapper.buildPublicKey(Optional.of(PUBLIC_KEY_ONLY));
        Optional<String> jwtToken = Optional.of(JWT_TOKEN);
        Pair<String, String> pair = verifier.verify(jwtPublicKey, jwtToken, CLOCK_SKEW_SECOND);
        assertEquals("Subject should be devops-admin@local", "devops-admin@local", pair.first);
        assertEquals("Role should be System Administrator", "System Administrator", pair.second);
    }

    /**
     * Verify latest JWT token with public key have prefix and suffix.
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testVerifyWithPrefixAndSuffixLatest() throws AuthenticationException {
        Optional<PublicKey> jwtPublicKey =
                mapper.buildPublicKeyLatest(Optional.of(PUBLIC_KEY_WITH_PREFIX_SUBFIX_ES256));
        Optional<String> jwtToken = Optional.of(JWT_ES256_TOKEN);
        Pair<String, String> pair =
                verifier.verifyLatest(jwtPublicKey, jwtToken, CLOCK_SKEW_SECOND);

        assertEquals("Subject should be devops-admin@local", "admin@local", pair.first);
        assertEquals("Role should be System Administrator", "Account Administrator", pair.second);
    }

    /**
     * Verify latest JWT token with public key that doesn't have prefix and suffix.
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testVerifyLatest() throws AuthenticationException {
        Optional<PublicKey> jwtPublicKey =
                mapper.buildPublicKeyLatest(Optional.of(PUBLIC_KEY_ES256));
        Optional<String> jwtToken = Optional.of(JWT_ES256_TOKEN);
        Pair<String, String> pair =
                verifier.verifyLatest(jwtPublicKey, jwtToken, CLOCK_SKEW_SECOND);

        assertEquals("Subject should be devops-admin@local", "admin@local", pair.first);
        assertEquals("Role should be System Administrator", "Account Administrator", pair.second);
    }

    /**
     * input: "Account Administrator", "Workload Optimizer Administrator", "Read-Only", "Workload
     * Optimizer Automator", "Workload Optimizer Deployer", "Workload Optimizer Advisor", "Workload
     * Optimizer Observer". output: "Account Administrator".
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetAcccountAdministratorRoles() throws AuthenticationException {
        final ImmutableList<String> list =
                ImmutableList.of("Account Administrator", WORKLOAD_OPTIMIZER_ADMINISTRATOR,
                        READ_ONLY, WORKLOAD_OPTIMIZER_AUTOMATOR, WORKLOAD_OPTIMIZER_DEPLOYER,
                        WORKLOAD_OPTIMIZER_ADVISOR, WORKLOAD_OPTIMIZER_OBSERVER);
        String jwt = createJWT(list);
        Pair<String, String> pair =
                verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Account Administrator", "Account Administrator", pair.second);

        jwt = createJWT(String.join(",", list));
        pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Account Administrator", "Account Administrator", pair.second);
    }

    /**
     * input: "Workload Optimizer Administrator", "Workload Optimizer Automator", "Workload
     * Optimizer Deployer", "Workload Optimizer Advisor", "Workload Optimizer Observer". output:
     * "Workload Optimizer Administrator".
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetAdministratorRoles() throws AuthenticationException {
        final ImmutableList<String> list =
                ImmutableList.of(WORKLOAD_OPTIMIZER_ADMINISTRATOR, WORKLOAD_OPTIMIZER_AUTOMATOR,
                        WORKLOAD_OPTIMIZER_DEPLOYER, WORKLOAD_OPTIMIZER_ADVISOR,
                        WORKLOAD_OPTIMIZER_OBSERVER);
        String jwt = createJWT(list);
        Pair<String, String> pair =
                verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Administrator",
                WORKLOAD_OPTIMIZER_ADMINISTRATOR, pair.second);
        jwt = createJWT(String.join(", ", list));
        pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Administrator",
                WORKLOAD_OPTIMIZER_ADMINISTRATOR, pair.second);
    }

    /**
     * input: "Workload Optimizer Administrator", "Device Administrator", "Workload Optimizer
     * Automator", "Workload Optimizer Deployer", "Workload Optimizer Advisor", "Workload Optimizer
     * Observer". output: "Workload Optimizer Administrator".
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetAdministratorRole() throws AuthenticationException {
        final ImmutableList<String> list =
                ImmutableList.of(WORKLOAD_OPTIMIZER_ADMINISTRATOR, DEVICE_ADMINISTRATOR,
                        WORKLOAD_OPTIMIZER_AUTOMATOR, WORKLOAD_OPTIMIZER_DEPLOYER,
                        WORKLOAD_OPTIMIZER_ADVISOR, WORKLOAD_OPTIMIZER_OBSERVER);
        String jwt = createJWT(list);
        Pair<String, String> pair =
                verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Administrator",
                WORKLOAD_OPTIMIZER_ADMINISTRATOR, pair.second);
        jwt = createJWT(String.join(", ", list));
        pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Administrator",
                WORKLOAD_OPTIMIZER_ADMINISTRATOR, pair.second);
    }

    /**
     * input: "Device Administrator", "Read-Only", "Workload Optimizer Automator", "Workload
     * Optimizer Deployer", "Workload Optimizer Advisor", "Workload Optimizer Observer". output:
     * "Read-Only"
     * output: "Read-Only"
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetReadOnlyRole() throws AuthenticationException {
        final ImmutableList<String> list =
                ImmutableList.of(DEVICE_ADMINISTRATOR, READ_ONLY, WORKLOAD_OPTIMIZER_AUTOMATOR,
                        WORKLOAD_OPTIMIZER_DEPLOYER, WORKLOAD_OPTIMIZER_ADVISOR,
                        WORKLOAD_OPTIMIZER_OBSERVER);
        String jwt = createJWT(list);
        Pair<String, String> pair =
                verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Read-Only", READ_ONLY, pair.second);
        jwt = createJWT(String.join(",", list));
        pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Read-Only", READ_ONLY, pair.second);

    }

    /**
     * input: "Device Administrator", "Workload Optimizer Automator", "Workload Optimizer Deployer",
     * "Workload Optimizer Advisor", "Workload Optimizer Observer". output: "Workload Optimizer
     * Automator"
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetAutomatorRole() throws AuthenticationException {
        final ImmutableList<String> list =
                ImmutableList.of(DEVICE_ADMINISTRATOR, WORKLOAD_OPTIMIZER_AUTOMATOR,
                        WORKLOAD_OPTIMIZER_DEPLOYER, WORKLOAD_OPTIMIZER_ADVISOR,
                        WORKLOAD_OPTIMIZER_OBSERVER);
        String jwt = createJWT(list);
        Pair<String, String> pair =
                verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Automator", WORKLOAD_OPTIMIZER_AUTOMATOR,
                pair.second);
        jwt = createJWT(String.join(",", list));
        pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Automator", WORKLOAD_OPTIMIZER_AUTOMATOR,
                pair.second);
    }

    /**
     * input: "Device Administrator", "Workload Optimizer Deployer", "Workload Optimizer Advisor",
     * "Workload Optimizer Observer". output: "Workload Optimizer Deployer".
     * output: "Workload Optimizer Deployer"
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetDeployerRole() throws AuthenticationException {
        final ImmutableList<String> list =
                ImmutableList.of(DEVICE_ADMINISTRATOR, WORKLOAD_OPTIMIZER_DEPLOYER,
                        WORKLOAD_OPTIMIZER_ADVISOR, WORKLOAD_OPTIMIZER_OBSERVER);
        String jwt = createJWT(list);
        Pair<String, String> pair =
                verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Deployer", WORKLOAD_OPTIMIZER_DEPLOYER,
                pair.second);
        jwt = createJWT(String.join(",", list));
        pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Deployer", WORKLOAD_OPTIMIZER_DEPLOYER,
                pair.second);
    }

    /**
     * input: "Device Administrator", "Workload Optimizer Advisor", "Workload Optimizer Observer".
     * output: "Workload Optimizer Advisor"
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetAdvisorRole() throws AuthenticationException {
        final ImmutableList<String> list =
                ImmutableList.of(DEVICE_ADMINISTRATOR, WORKLOAD_OPTIMIZER_ADVISOR,
                        WORKLOAD_OPTIMIZER_OBSERVER);
        String jwt = createJWT(list);
        Pair<String, String> pair =
                verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Advisor", WORKLOAD_OPTIMIZER_ADVISOR,
                pair.second);
        jwt = createJWT(String.join(",", list));
        pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Advisor", WORKLOAD_OPTIMIZER_ADVISOR,
                pair.second);
    }

    /**
     * input: "Device Administrator", "Workload Optimizer Observer".
     * output: "Workload Optimizer Observer"
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetObserverRole() throws AuthenticationException {
        final ImmutableList<String> list =
                ImmutableList.of(DEVICE_ADMINISTRATOR, WORKLOAD_OPTIMIZER_OBSERVER);
        String jwt = createJWT(list);
        Pair<String, String> pair =
                verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Observer", WORKLOAD_OPTIMIZER_OBSERVER,
                pair.second);
        jwt = createJWT(String.join(",", list));
        pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Observer", WORKLOAD_OPTIMIZER_OBSERVER,
                pair.second);
    }

    /**
     * input: "Workload Optimizer Observer"
     *  output: "Workload Optimizer Observer"
     *
     * @throws AuthenticationException if verification failed.
     */
    @Test
    public void testGetSingleCustomRole() throws AuthenticationException {
        String jwt = createJWT(WORKLOAD_OPTIMIZER_OBSERVER);
        Pair<String, String> pair = verifier.verifyLatest(Optional.of(signingKey), Optional.of(jwt), CLOCK_SKEW_SECOND);
        assertEquals("Role should be Workload Optimizer Observer", WORKLOAD_OPTIMIZER_OBSERVER,
                pair.second);
    }
}