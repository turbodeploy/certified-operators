package com.vmturbo.components.common.config;

import static com.vmturbo.components.common.config.SecretPropertiesReader.CLIENT_ID;
import static com.vmturbo.components.common.config.SecretPropertiesReader.CLIENT_SECRET;
import static com.vmturbo.components.common.config.SecretPropertiesReader.PASSWORD;
import static com.vmturbo.components.common.config.SecretPropertiesReader.USERNAME;
import static com.vmturbo.components.common.config.SecretPropertiesReader.componentSecretKeyMap;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.vmturbo.components.common.config.SecretPropertiesReader.MissingSecretEntry;
import com.vmturbo.components.common.config.SecretPropertiesReader.UnknowComponentException;

/**
 * Verify {@link SecretPropertiesReader}.
 */
@RunWith(Parameterized.class)
public class SecretPropertiesReaderTest {
    private static final String secretsDir = "secretMap";    // relative to test/resources
    private static final Map<String, String> expectedSecretsByKey = ImmutableMap.of(
            USERNAME, "v-kubernetes-coke-plan--3AvWqRZs",
            PASSWORD, "A1a-9Y9tLPAX2NXOJYb9",
            CLIENT_ID, "b98b4668a6311a1849384b90efc6ab341cf800395d123dbd9f5e22a7d03e924c",
            CLIENT_SECRET, "2fee1ab9957086015883d4aa7733b92f"
    );
    private final String componentType;
    private final String secretFilePath;

    /**
     * Constructor.
     *
     * @param componentType component type.
     * @param secretFile the file name in the resource directory that has the test secrets
     */
    public SecretPropertiesReaderTest(final String componentType, final String secretFile) {
        this.componentType = componentType;
        this.secretFilePath = secretsDir + File.separatorChar + secretFile;
    }

    /**
     * Parameters setup.
     *
     * @return list of parameters.
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"action-orchestrator", "sample_file_username_password_secret"},
                {"auth", "sample_file_username_password_secret"},
                {"cost", "sample_file_username_password_secret"},
                {"group", "sample_file_username_password_secret"},
                {"history", "sample_file_username_password_secret"},
                {"plan-orchestrator", "sample_file_username_password_secret"},
                {"topology-processor", "sample_file_username_password_secret"},
                {"repository", "sample_file_username_password_secret"},
                {"intersight-integration", "sample_file_client_id_secret"}
        });
    }

    /**
     * Positive case.
     *
     * @throws IOException when IO exception thrown
     */
    @Test
    public void testReadSecretFile() throws IOException {
        // act
        Properties result = SecretPropertiesReader.readSecretFile(componentType, secretFilePath);
        // assert
        final Map<String, String> secretsMap = componentSecretKeyMap.get(componentType);
        for (final Map.Entry<String, String> entry : secretsMap.entrySet()) {
            final String propName = entry.getValue();
            final Object actualSecretValue = result.get(propName);
            final String expectedSecretValue = expectedSecretsByKey.get(entry.getKey());
            assertEquals(expectedSecretValue, actualSecretValue);
        }
    }

    /**
     * Negative case, missing required key (password).
     *
     * @throws IOException when IO exception thrown
     */
    @Test(expected = MissingSecretEntry.class)
    public void testReadIncompleteSecretFile() throws IOException {
        // act
        SecretPropertiesReader.readSecretFile(componentType,
                secretsDir + File.separatorChar + "sample_file_missing_part_of_secret");
    }

    /**
     * Negative case, component type is not define in {@link SecretPropertiesReader#componentSecretKeyMap}.
     *
     * @throws IOException when IO exception thrown
     */
    @Test(expected = UnknowComponentException.class)
    public void testReadSecretFileInUnknownComponent() throws IOException {
        // act
        SecretPropertiesReader.readSecretFile("unknown",
                secretsDir + File.separatorChar + "sample_file_missing_part_of_secret");
    }

    /**
     * Negative case, missing the secret file.
     *
     * @throws IOException when IO exception thrown
     */
    @Test(expected = FileNotFoundException.class)
    public void testReadMissingSecretFile() throws IOException {
        SecretPropertiesReader.readSecretFile(componentType,
                secretsDir + File.separatorChar + "non_existing_secret_file");
    }
}
