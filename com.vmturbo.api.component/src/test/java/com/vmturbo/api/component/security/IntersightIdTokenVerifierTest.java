package com.vmturbo.api.component.security;

import static org.springframework.test.util.AssertionErrors.assertEquals;

import java.security.PublicKey;
import java.util.Collections;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.authentication.AuthenticationException;

/**
 * Test for {@link IntersightIdTokenVerifier}.
 */
public class IntersightIdTokenVerifierTest {

    /**
     * public key provided by Cisco with prefix and suffix.
     */
    public static final String PUBLIC_KEY_WITH_PREFIX_SUFFIX =
            "-----BEGIN RSA PUBLIC KEY----- MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA10XjWtx/V6pXPUh0RK+b QycdKJlRlTxzPkyUXPTTD5Q7aUdNpNHki8H3dv643dDuH133ABU0BC4Zh/TjqfdU Op7Bd72VrE/HGStqcG1pxydYbUaSC3B9uAQAEAyuWuT38aCOrPjTiSmNCdo9ZGT9 Y70GjXcUbuXKo05UtzqC2vrQGcEQu2+0N+ngpaMT9yFExNuwOvNNM4BEEL1VR35n vOVjkmjmFpvKHkzZWABJ8aoK5E3b1ABcDwvyHdnk4lP0DjqsW9XjciMZ+Ov6fUn3 ltpt4AFBvD4korkzc1qF7WcArrXUmn6xvpw78rcyMVI23i2SMLBTXk+efGbSmtqC 19VqtbbxfVdLftcNNUM2I+5QoiAp8RNlDqWh2gUq8335sLLjsEL+sbMcMLFtml0b Ym8//ufQzyMZph43To1PIMyuDfxUXFa97DJBsHE5T4e4k3yb9mejYpDDL3iOGs1f 3/YRVmRBrSGjEXcyqo1lttm5mbunlo5018qO4TnlyKGeanE3wuuHJ6iWVv36QQgz qDqN0xOeixcdbRXX7RyKlAeMGrioOnF+sdIuk2Nh3xzVxE+cUTfZPDXvBUyoQP3J bY7x6uCr56d9UX6HAgQnW+ErkrrMLiU4Az8W8W2DwjSdanP7X6Jcq2PAIOQxRp27 v1kw5LWSaUk0x8bC2jfDclcCAwEAAQ== -----END RSA PUBLIC KEY-----";
    /**
     * public key provided by Cisco without prefix and suffix.
     */
    public static final String PUBLIC_KEY_ONLY =
            "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA10XjWtx/V6pXPUh0RK+b QycdKJlRlTxzPkyUXPTTD5Q7aUdNpNHki8H3dv643dDuH133ABU0BC4Zh/TjqfdU Op7Bd72VrE/HGStqcG1pxydYbUaSC3B9uAQAEAyuWuT38aCOrPjTiSmNCdo9ZGT9 Y70GjXcUbuXKo05UtzqC2vrQGcEQu2+0N+ngpaMT9yFExNuwOvNNM4BEEL1VR35n vOVjkmjmFpvKHkzZWABJ8aoK5E3b1ABcDwvyHdnk4lP0DjqsW9XjciMZ+Ov6fUn3 ltpt4AFBvD4korkzc1qF7WcArrXUmn6xvpw78rcyMVI23i2SMLBTXk+efGbSmtqC 19VqtbbxfVdLftcNNUM2I+5QoiAp8RNlDqWh2gUq8335sLLjsEL+sbMcMLFtml0b Ym8//ufQzyMZph43To1PIMyuDfxUXFa97DJBsHE5T4e4k3yb9mejYpDDL3iOGs1f 3/YRVmRBrSGjEXcyqo1lttm5mbunlo5018qO4TnlyKGeanE3wuuHJ6iWVv36QQgz qDqN0xOeixcdbRXX7RyKlAeMGrioOnF+sdIuk2Nh3xzVxE+cUTfZPDXvBUyoQP3J bY7x6uCr56d9UX6HAgQnW+ErkrrMLiU4Az8W8W2DwjSdanP7X6Jcq2PAIOQxRp27 v1kw5LWSaUk0x8bC2jfDclcCAwEAAQ==";

    /**
     * Sample JTW token provided by Cisco.
     */
    public static final String JWT_TOKEN =
            "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJBY2NvdW50IjoiNWRjMjI2YmM3NTY0NjEyZDMwNWU2MGYwIiwiQWNjb3VudE5hbWUiOiJzZXJ2aWNlLWFkbWluIiwiVXNlciI6IjVkYzIyYTJjNzU2NDYxMmQzMDVlNjg0OCIsIlJvbGVzIjoiU3lzdGVtIEFkbWluaXN0cmF0b3IiLCJSb2xlSWRzIjoiNWRjMjI2YmM3NTY0NjEyZDMwNWU2MTA4IiwiRXBEZXZpY2VUeXBlIjoiIiwiRXBQcml2aWxlZ2VzIjoiIiwiU2VydmljZUFkbWluIjoieWVzIiwiRG9tYWluR3JvdXAiOiI1ZGMyMjZiYzc1NjQ2MTJkMzA1ZTYwZjEiLCJEb21haW5Hcm91cFBhcnRLZXkiOiJINWRjMjI2YmM3NTY0NjEyZDMwNWU2MGYxIiwiUHJveHlJbnN0YW5jZVBhcnRpdGlvbiI6IiIsIlBlcm1pc3Npb25JZCI6IjVkYzIyNmJjNzU2NDYxMmQzMDVlNjEwYiIsIlBlcm1pc3Npb25OYW1lIjoiU3lzdGVtIEFkbWluaXN0cmF0b3IiLCJhdWQiOiJJTyxCYXJjZWxvbmEiLCJleHAiOjE1NzM2Mjc2MTksImlhdCI6MTU3MzYyNTgxOSwiaXNzIjoiYmFycmFjdWRhIiwic3ViIjoiZGV2b3BzLWFkbWluQGxvY2FsIn0.cwD-QJjHTDZy7km7d3yJ2EupZ-C-X5EoZRSuSwuox1Jjbs3yD00zdRFkYSyokeKE-6lFb50Lvc_ghN_MlLpuwnilUcihZS5CHlESI9MnZL8tyPLo0aQ3-_bteEoJPSOS8QANhL5oUY2MOnrupiaf90_CCqwR550Ixuk2f0u5-uAUWdxMUXpK18S8jmDp5KhDdCuwv7UvRr4KuiGIBGg3EmcCP_OotGmm9nPT6pFd4GSTVPW4PLgihip4LO1AcUM__W7VVQ4w3CXK6JT-nitLG7KHaQjbOr5uixs07mKJLRfzb8DugEstxs8HBq4gmuaeyYOuAVGrATiqKCkfcdrP6vgcA2Dk7_GlFYSuDJ8v7OCwo4NcPHTQ_Ib-YbizF_CRF3aLDi7b0B2f31s3dQFXeymehLxJpUc-dJa7We_0JjBySijQPsMTZW4kYHFyhp3pBEkcKiRhtG2D2EBvJdY-A2DDnj8fbaPvd27MFibY9_RwlVs9hL-X1RgvLhOhFwDxFoieQHLeRycJ0YpXvkFEsBM6DA3yZ_pIZFXIAr-bLKdFe50ksOfW1bMAxZ8Z2bRCyrzMR4ZMu51XAIvyMnE8W-jZSDs4L7So1Ddre0Hshc7VZ2gVOGE4JKnfRBJwOGTwxYlM9RuTd2bXl3F3JrUJIHyAaSwPdKo0kEtCKLN4Qfo";
    private static final int CLOCK_SKEW_SECOND = 60 * 60 * 24 * 365 * 30; // 30 years
    private static final String RSA_PUBLIC_KEY_PREFIX = "-----BEGIN RSA PUBLIC KEY----- ";
    private static final String RSA_PUBLIC_KEY_SUFFIX = " -----END RSA PUBLIC KEY-----";
    private IntersightIdTokenVerifier verifier;
    private HeaderMapper mapper =
            new IntersightHeaderMapper(Collections.emptyMap(), "", "", "", "");

    /**
     * Before every test.
     */
    @Before
    public void setup() {
        verifier = new IntersightIdTokenVerifier();
    }

    /**
     * Verify JWT token with public key have prefix and suffix.
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
     * Verify JWT token with public key that doesn't have prefix and suffix.
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
}