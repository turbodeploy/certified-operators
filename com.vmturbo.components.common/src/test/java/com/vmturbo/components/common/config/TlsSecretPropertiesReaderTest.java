package com.vmturbo.components.common.config;

import static com.vmturbo.components.common.config.TlsSecretPropertiesReader.TLS_KEYSTORE;
import static com.vmturbo.components.common.config.TlsSecretPropertiesReader.TLS_KEYSTORE_PASS;
import static com.vmturbo.components.common.config.TlsSecretPropertiesReader.TLS_KEY_PASS;
import static com.vmturbo.components.common.config.TlsSecretPropertiesReader.TLS_TRUSTSTORE;
import static com.vmturbo.components.common.config.TlsSecretPropertiesReader.TLS_TRUSTSTORE_PASS;
import static com.vmturbo.components.common.config.TlsSecretPropertiesReader.TlS_SECRET_SET;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

/**
 * Verify {@link TlsSecretPropertiesReader}.
 */
public class TlsSecretPropertiesReaderTest {
    private static final String secretsDir = "secretMap";    // relative to test/resources

    private static final Map<String, String> expectedTlsSecretsByKey = ImmutableMap.of(TLS_KEYSTORE,
            "/vault/key/keystore", TLS_KEYSTORE_PASS, "confluent", TLS_KEY_PASS, "confluent",
            TLS_TRUSTSTORE, "/vault/trust/truststore", TLS_TRUSTSTORE_PASS, "confluent");
    private final String secretFilePath =
            secretsDir + File.separatorChar + "sample_file_TLS_secret";

    /**
     * Positive case.
     *
     * @throws IOException when IO exception thrown
     */
    @Test
    public void testReadSecretFile() throws IOException {
        // act
        Properties result = TlsSecretPropertiesReader.readSecretFile(secretFilePath);

        for (String string : TlS_SECRET_SET) {
            final String expectedSecretValue = expectedTlsSecretsByKey.get(string);
            final Object actualSecretValue = result.get(string);
            assertEquals(expectedSecretValue, actualSecretValue);
        }
    }

    /**
     * Negative case, missing the secret file.
     *
     * @throws IOException when IO exception thrown
     */
    @Test(expected = FileNotFoundException.class)
    public void testReadMissingSecretFile() throws IOException {
        TlsSecretPropertiesReader.readSecretFile(
                secretsDir + File.separatorChar + "non_existing_secret_file");
    }
}
