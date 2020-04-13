package com.vmturbo.components.common.config;

import static com.vmturbo.components.common.config.SecretPropertiesReader.PASSWORD;
import static com.vmturbo.components.common.config.SecretPropertiesReader.USERNAME;
import static com.vmturbo.components.common.config.SecretPropertiesReader.componentCredentialKeyMap;
import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

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

    private final String componentType;

    /**
     * Constructor.
     *
     * @param componentType component type.
     */
    public SecretPropertiesReaderTest(String componentType) {
        this.componentType = componentType;
    }

    /**
     * Parameters setup.
     *
     * @return list of parameters.
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"action-orchestrator"}, {"auth"}, {"cost"}, {"group"}, {"history"},
                {"plan-orchestrator"}, {"topology-processor"}, {"repository"}
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
        Properties result = SecretPropertiesReader.readSecretFile(componentType,
                "secretMap/sample_secrets");
        // assert
        assertEquals("A1a-9Y9tLPAX2NXOJYb9", result.get(
                componentCredentialKeyMap.get(componentType).get(PASSWORD)));
        assertEquals("v-kubernetes-coke-plan--3AvWqRZs", result.get(
                componentCredentialKeyMap.get(componentType).get(USERNAME)));
    }

    /**
     * Negative case, missing required key (password).
     *
     * @throws IOException when IO exception thrown
     */
    @Test(expected = MissingSecretEntry.class)
    public void testReadIncompleteSecretFile() throws IOException {
        // act
        Properties result =
                SecretPropertiesReader.readSecretFile(componentType,
                        "secretMap/sample_secrets_missing_password");
    }

    /**
     * Negative case, component type is not define in {@link SecretPropertiesReader#componentCredentialKeyMap}.
     *
     * @throws IOException when IO exception thrown
     */
    @Test(expected = UnknowComponentException.class)
    public void testReadSecretFileInUnknownComponent() throws IOException {
        // act
        Properties result =
                SecretPropertiesReader.readSecretFile("unknown",
                        "secretMap/sample_secrets_missing_password");
    }

    /**
     * Negative case, missing the secret file.
     *
     * @throws IOException when IO exception thrown
     */
    @Test(expected = FileNotFoundException.class)
    public void testReadMissingSecretFile() throws IOException {
        SecretPropertiesReader.readSecretFile(componentType, "secretMap/notExistedSample_secrets");
    }
}