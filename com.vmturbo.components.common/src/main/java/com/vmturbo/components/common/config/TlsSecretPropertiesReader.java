package com.vmturbo.components.common.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.config.YamlProcessor;
import org.springframework.core.CollectionFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

/**
 * mTLS secret property reader.
 */
public class TlsSecretPropertiesReader extends YamlProcessor {

    static final String TLS_KEYSTORE = "TLS_KEYSTORE";
    static final String TLS_KEYSTORE_PASS = "TLS_KEYSTORE_PASS";
    static final String TLS_KEY_PASS = "TLS_KEY_PASS";
    static final String TLS_TRUSTSTORE = "TLS_TRUSTSTORE";
    static final String TLS_TRUSTSTORE_PASS = "TLS_TRUSTSTORE_PASS";

    static final Set<String> TlS_SECRET_SET = ImmutableSet.of(TLS_KEYSTORE, TLS_KEYSTORE_PASS,
            TLS_KEY_PASS, TLS_TRUSTSTORE, TLS_TRUSTSTORE_PASS);

    private static final Logger logger = LogManager.getLogger();

    /**
     * Read the YAML comparable secret file at the given file path, and return as {@link
     * Properties}.
     *
     * @param propertiesYamlPath the file path to the properties.yaml file to be loaded
     * @return a new {@link Properties} object with a merged set of key/value properties
     *         corresponding to the default and custom properties, global and for this instance-type
     * @throws IOException if the properties.yaml path cannot be read
     */
    public static Properties readSecretFile(@Nonnull final String propertiesYamlPath)
            throws IOException {
        final Properties result = CollectionFactory.createStringAdaptingProperties();
        final TlsSecretPropertiesReader propertiesReader = new TlsSecretPropertiesReader();
        final Resource propertiesYamlResource = new DefaultResourceLoader().getResource(
                propertiesYamlPath);
        final Path filePath = Paths.get(propertiesYamlResource.getFile().getPath());

        if (!Files.exists(filePath)) {
            throw new FileNotFoundException("Missing file" + filePath);
        }
        propertiesReader.setResources(propertiesYamlResource);
        try {
            propertiesReader.process(
                    (properties, map) -> result.putAll(propertiesReader.extractProperties(map)));
        } catch (IllegalStateException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw e;
        }
        return result;
    }

    private Properties extractProperties(@Nonnull final Map<String, Object> propertiesYamlMap) {
        final Properties result = new Properties();
        for (String secretKey : TlS_SECRET_SET) {
            Object secretValue = propertiesYamlMap.get(secretKey);
            if (secretValue != null) {
                result.put(secretKey, secretValue);
                logger.info("Loading secret: {}", secretKey);
            } else {
                logger.info("Failed Loading secret: {}", secretKey);
            }
        }
        return result;
    }
}
