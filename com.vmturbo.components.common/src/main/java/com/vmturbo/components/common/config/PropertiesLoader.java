package com.vmturbo.components.common.config;

import static com.vmturbo.components.common.BaseVmtComponent.PROP_COMPONENT_TYPE;
import static com.vmturbo.components.common.BaseVmtComponent.PROP_PROPERTIES_YAML_PATH;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;
import com.vmturbo.components.common.utils.EnvironmentUtils;

/**
 * Load the configuration properties from "properties.yaml" and the "other" configuration properties
 * into PropertySources and add those to the given ApplicationContext.
 */
public class PropertiesLoader {

    private static final Logger logger = LogManager.getLogger();

    private static final String DEFAULT_PROPERTIES_YAML_FILE_PATH =
        "file:/etc/turbonomic/properties.yaml";
    private static final String CONFIG = "config";
    /**
     * The config source name for the properties read from "properties.yaml".
     */
    private static final String PROPERTIES_YAML_CONFIG_SOURCE = "properties.yaml";
    private static final String COMPONENT_DEFAULT_PATH = CONFIG + "/component_default.properties";
    /**
     * The config source name for the properties read from the "CONFIG" resource.
     */
    private static final String OTHER_PROPERTIES_CONFIG_SOURCE = "other-properties";

    private PropertiesLoader() {
    }

    /**
     * Load the configuration properties from "properties.yaml" and the "other" configuration
     * properties into PropertySources and add those to the given ApplicationContext.
     *
     * @param applicationContext the context for the component to be configured
     * @throws ContextConfigurationException if there is an error reading any of the property
     * configuration sources
     */
    public static void addConfigurationPropertySources(
        @Nonnull final AnnotationConfigWebApplicationContext applicationContext)
            throws ContextConfigurationException {
        // Fetch external configuration properties from  to add to context
        String propertiesYamlFilePath = EnvironmentUtils
            .getOptionalEnvProperty(PROP_PROPERTIES_YAML_PATH)
            .orElse(DEFAULT_PROPERTIES_YAML_FILE_PATH);
        final String componentType = applicationContext.getEnvironment().getRequiredProperty(PROP_COMPONENT_TYPE);
        final PropertySource<?> mergedPropertyConfiguration =
            fetchConfigurationProperties(componentType, propertiesYamlFilePath);
        applicationContext.getEnvironment().getPropertySources()
            .addFirst(mergedPropertyConfiguration);
        // Fetch other configuration properties from files compiled into the component
        applicationContext.getEnvironment().getPropertySources()
            .addFirst(fetchOtherProperties(CONFIG));
    }

    /**
     * Fetch the Turbonomic external configuration properties for this component.
     *
     * <p>The configuration properties are fetched from the "properties.yaml" file mounted
     * from the K8s ConfigMap resource. This includes defaultProperties and customProperties
     * sections with (optional) override sections for each component-type.
     *
     * <p>The "customProperties" section is populated from the Custom Resource configuration
     * for this particular Turbonomic deployment.
     *
     * <p>The "effective" configuration properties are calculated by merging the different sections
     * of "properties.yaml" in priority order:
     * <ol>
     *     <li>defaultProperties: global:
     *     <li>defaultProperties: [component-type]:
     *     <li>customProperties: global:</li>
     *     <li>customProperties: [component-type]:
     * </ol>
     *
     * @param componentType The type of the component to be configured, used to look up the
     *                      subsection of the properties.yaml file
     * @param propertiesYamlFilePath the file path to fetch the "properties.yaml" file from
     * @return a PropertySource containing the configuration properties loaded from the
     * given configuration file path
     * @throws ContextConfigurationException if there is a problem reading the "properties.yaml"
     * file or the file has an invalid structure
     */
    @VisibleForTesting
    static PropertySource<?> fetchConfigurationProperties(
        @Nonnull final String componentType,
        @Nonnull final String propertiesYamlFilePath) throws ContextConfigurationException {
        try {
            final Properties yamlProperties = ConfigMapPropertiesReader.readConfigMap(
                componentType, propertiesYamlFilePath);
            // log the properties for debugging
            logger.info("Configuration properties loaded from properties.yaml: {}",
                propertiesYamlFilePath);
            yamlProperties.forEach(PropertiesLoader::logProperty);
            // populate a PropertySource with the config properties from the yaml file
            return new PropertiesPropertySource(PROPERTIES_YAML_CONFIG_SOURCE, yamlProperties);
        } catch (IOException e) {
            throw new ContextConfigurationException("Error reading configuration file: " +
                propertiesYamlFilePath, e);
        }
    }

    /**
     * Fetch configuration properties other than {@link #COMPONENT_DEFAULT_PATH}.
     * Look for files in the "config" resource. Files of type ".properties" are
     * treated as {@link Properties} files. For other file types create a property
     * which name is the file name and value is the content of the file.
     *
     * @param otherPropertiesResource the name of the "resource" to fetch the "other"
     *                                configuration properties from
     * @return a properties map with all the loaded properties
     * @throws ContextConfigurationException when there is a problem accessing resources
     */
    @VisibleForTesting
    static PropertySource<?> fetchOtherProperties(
        @Nonnull final String otherPropertiesResource) throws ContextConfigurationException {
        try {
            Properties properties = new Properties();
            Enumeration<URL> configs = BaseVmtComponent.class.getClassLoader()
                .getResources(otherPropertiesResource);
            while (configs.hasMoreElements()) {
                URI uri = configs.nextElement().toURI();
                FileSystem fs = fileSystem(uri);
                Path configPath = fs.getPath(path(uri));
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(configPath)) {
                    ds.forEach(propPath -> {
                        // Skip COMPONENT_DEFAULT_PATH - it is loaded in loadDefaultProperties()
                        if (!propPath.toString().endsWith(COMPONENT_DEFAULT_PATH)) {
                            String fileName = propPath.getFileName().toString();
                            if (fileName.endsWith(".properties")) {
                                properties.putAll(propsFromInputStream(
                                    pathInputStream(propPath), propPath.toString()));
                            } else {
                                logger.info("Loading " + propPath);
                                try {
                                    String content = new String(Files.readAllBytes(propPath));
                                    properties.put(fileName, content);
                                    logger.info("Loaded " + content.length()
                                        + " bytes from " + propPath);
                                } catch (IOException e) {
                                    logger.warn("Could not load " + propPath);
                                }
                            }
                        }
                    });
                } finally {
                    try {
                        fs.close();
                    } catch (UnsupportedOperationException usoe) {
                        // Happens during testing with "file" scheme. Ignore.
                    }
                }
            }
            return new PropertiesPropertySource(OTHER_PROPERTIES_CONFIG_SOURCE, properties);
        } catch (URISyntaxException | IOException e) {
            throw new ContextConfigurationException("Error reading other properties files from: " +
                otherPropertiesResource, e);
        }
    }

    private static Supplier<InputStream> pathInputStream(Path propPath) {
        return () -> {
            try {
                return Files.newInputStream(propPath);
            } catch (IOException e) {
                return null;
            }
        };
    }

    private static Properties propsFromInputStream(Supplier<InputStream> isSupplier,
                                                   String pathName) {
        logger.info("Loading properties from " + pathName);
        Properties props = new Properties();
        try (InputStream is = isSupplier.get()) {
            props.load(is);
            int numProps = props.size();
            String propCount = numProps + (numProps == 1 ? " property" : " properties");
            logger.info("Loaded " + propCount + " from " + pathName);
        } catch (IOException ioe) {
            logger.warn("Could not load properties from " + pathName);
        }
        return props;
    }

    private static FileSystem fileSystem(URI uri) throws IOException {
        return "file".equals(uri.getScheme())
            // "file" scheme used in unit tests
            ? FileSystems.getDefault()
            // "jar" scheme expected at runtime
            : FileSystems.newFileSystem(
                URI.create(uri.toString().replaceFirst("!.*", "")), Collections.emptyMap());
    }

    private static String path(URI uri) {
        String path = "file".equals(uri.getScheme())
            // "file" scheme used in unit tests
            ? uri.getPath()
            // "jar" scheme expected at runtime
            : uri.toString().replaceFirst(".*!", "");
        return StringUtils.isNotEmpty(path) && SystemUtils.IS_OS_WINDOWS && path.startsWith("/") && path.contains(":")
                        ? path.substring(1)
                        : path;
    }

    /**
     * Log a pair of key-value properties. If the value is too long or
     * is multi-lines then print the first few characters followed by ...
     *
     * @param key a property key
     * @param value a property value
     */
    private static void logProperty(Object key, Object value) {
        String str;
        if (value == null) {
            str = null;
        } else {
            str = value.toString();
            int originalLength = str.length();
            boolean truncated = false;
            if (str.length() > 80) {
                truncated = true;
                str = str.substring(0, 76);
            }
            if (str.contains("\n")) {
                truncated = true;
                str = str.substring(0, str.indexOf("\n"));
            }
            if (truncated) {
                str += "... [" + (originalLength - str.length() + " bytes truncated]");
            }
        }
        logger.info("       {} = '{}'", key, str);
    }
}
