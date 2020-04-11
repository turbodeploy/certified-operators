package com.vmturbo.components.common.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.config.YamlProcessor;
import org.springframework.core.CollectionFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

/**
 * Read the YAML format secret file from the file system. For example, extracting following
 * key/value from the secret file:
 * <pre>
 * password: A1a-9Y9tLPAX2NXOJYb9
 * username: v-kubernetes-coke-plan--3AvWqRZs
 * </pre>
 * And return as credential proerties that match the corresponding components, see componentCredentialKeyMap for the mapping.
 */
public class SecretPropertiesReader extends YamlProcessor {
    /**
     * Secret: username.
     */
    @VisibleForTesting
    static final String USERNAME = "username";
    /**
     * Secret: password.
     */
    @VisibleForTesting
    static final String PASSWORD = "password";
    /**
     * Secret: ClientId.
     */
    @VisibleForTesting
    static final String CLIENT_ID = "ClientId";
    /**
     * Secret: ClientSecret.
     */
    @VisibleForTesting
    static final String CLIENT_SECRET = "ClientSecret";

    private static final Logger logger = LogManager.getLogger();

    /**
     * component type -> (componentUserKey, componentPassKey). These mapping are defined in Spring config file, e.g.:
     * AuthDBConfig.java for Auth component.
     */
    @VisibleForTesting
    static final Map<String, Map> componentSecretKeyMap = ImmutableMap.<String, Map>builder()
            .put("action-orchestrator", ImmutableMap.of(USERNAME, "actionDbUsername", PASSWORD, "actionDbPassword"))
            .put("auth", ImmutableMap.of(USERNAME, "authDbUsername", PASSWORD, "authDbPassword"))
            .put("cost", ImmutableMap.of(USERNAME, "costDbUsername", PASSWORD, "costDbPassword"))
            .put("group", ImmutableMap.of(USERNAME, "groupComponentDbUsername", PASSWORD, "groupComponentDbPassword"))
            .put("history", ImmutableMap.of(USERNAME, "historyDbUsername", PASSWORD, "historyDbPassword"))
            .put("plan-orchestrator", ImmutableMap.of(USERNAME, "planDbUsername", PASSWORD, "planDbPassword"))
            .put("topology-processor", ImmutableMap.of(USERNAME, "topologyProcessorDbUsername", PASSWORD, "topologyProcessorDbPassword"))
            .put("repository", ImmutableMap.of(USERNAME, "arangoDBUsername", PASSWORD, "arangoDBPassword")) // see RepositoryProperties
            .put("intersight-integration", ImmutableMap.of(CLIENT_ID, "intersightClientId", CLIENT_SECRET, "intersightClientSecret"))
            .build();

    /**
     * Read the YAML comparable secret file at the given file path, and return as {@link
     * Properties}.
     *
     * @param componentType component type
     * @param propertiesYamlPath the file path to the properties.yaml file to be loaded
     * @return a new {@link Properties} object with a merged set of key/value properties
     * corresponding to the default and custom properties, global and for this instance-type
     * @throws IOException if the properties.yaml path cannot be read
     */
    public static Properties readSecretFile(@Nonnull String componentType, @Nonnull final String propertiesYamlPath)
            throws IOException {
        final Properties result = CollectionFactory.createStringAdaptingProperties();
        final SecretPropertiesReader propertiesReader = new SecretPropertiesReader();
        final Resource propertiesYamlResource =
                new DefaultResourceLoader().getResource(propertiesYamlPath);
        final Path filePath = Paths.get(propertiesYamlResource.getFile().getPath());

        if (!Files.exists(filePath)) {
            // TODO (Gary, Mar 2020) reconsider throwing exception. If secret file is not mandatory,
            //  it's better don't thrown exception.
            throw new FileNotFoundException("Missing file" + filePath);
        }
        propertiesReader.setResources(propertiesYamlResource);
        try {
            propertiesReader.process(
                    (properties, map) -> result.putAll(propertiesReader.extractProperties(componentType, map)));
        } catch (MissingSecretEntry | IllegalStateException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw e;
        }
        return result;
    }

    /**
     * Extract the global and component-type specific properties from the properties.yaml map and
     * combine them into a resulting Properties object.
     *
     * @param componentType component type
     * @param propertiesYamlMap the Map deserialized from properties.yaml
     * @return a Properties object containing the global and component-type specific properties
     * @throws MissingSecretEntry exception if the expected secret is not found in the file.
     */
    private Properties extractProperties(@Nonnull String componentType,
                                         @Nonnull final Map<String, Object> propertiesYamlMap) {
        final Properties result = new Properties();
        final Map<String, String> secretsMap = componentSecretKeyMap.get(componentType);
        if (secretsMap == null) {
            logger.error("Found unknown component: {}. If it's a valid component, add it to " +
                    "SecretPropertiesReader#componentCredentialKeyMap", componentType);
            throw new UnknowComponentException("Found unknown component: " + componentType);
        }
        for (Map.Entry<String, String> secretEntry : secretsMap.entrySet()) {
            final String secretKey = secretEntry.getKey();
            Object secretValue = propertiesYamlMap.get(secretKey);
            if (secretValue != null) {
                final String componentPropName = secretEntry.getValue();
                result.put(componentPropName, secretValue);
                logger.info("Loading secret: {}", secretKey);
            } else {
                logger.error("Cannot find value for secret key: {}", secretKey);
                throw new MissingSecretEntry("Missing secret entry: " + secretKey);
            }
        }
        return result;
    }

    /**
     * Exception when the required key is not found in the secret file.
     */
    @VisibleForTesting
    class MissingSecretEntry extends RuntimeException {
        MissingSecretEntry(@Nonnull final String msg) {
            super(msg);
        }
    }

    /**
     * Exception when the component type is not recognized.
     */
    @VisibleForTesting
    class UnknowComponentException extends RuntimeException {
        UnknowComponentException(@Nonnull final String msg) {
            super(msg);
        }
    }
}
