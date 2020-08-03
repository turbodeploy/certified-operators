package com.vmturbo.components.common.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.config.YamlProcessor;
import org.springframework.core.CollectionFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

/**
 * Read the properties.yaml file from the file system. Merge the configuration properties for a
 * given instance-type with global properties taken from the following yaml sub-sections,
 * in order, with "last definition of a particular property key wins".
 *
 * <p>The different segments of the properties.yaml file are:
 * <ol>
 *     <li>defaultProperties: global:</li>
 *     <li>defaultProperties: {instance-type}: </li>
 *     <li>customProperties: global</li>
 *     <li>customProperties: {instance-type}</li>
 * </ol>
 * For example, extracting the properties for "topology-processor" from the following yaml:
 * <pre>
 *     defaultProperties:
 *       global:
 *          foo: 123
 *          bar: 456
 *       topology-processor:
 *          xyz: 123
 *       group:
 *          numGroupMax: 1000
 *     customProperties:
 *       global:
 *          bar: 789
 *          new: 'this is new'
 *       topology-processor:
 *          xyz: 999
 * </pre>
 * would result in the merged property list:
 * <pre>
 *     foo: '123'
 *     bar: '789'
 *     new: 'this is new'
 *     xyz: '999'
 * </pre>
 * Note that the property values are all converted to String.
 */
public class ConfigMapPropertiesReader extends YamlProcessor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * These are the two subkeys to look for, in order, when extracting property values
     * from "properties.yaml".
     */
    private static final String[] PROP_SOURCE_SUBKEYS = {"defaultProperties", "customProperties"};

    /**
     * Read the YAML file at the given file path, and extract the Properties combining the
     * defaultProperties:global, defaultProperties:{instance-type}.
     *
     * @param componentType the specific component-type to extract from the properties.yaml to be
     *                      combined with the global properties
     * @param propertiesYamlPath the file path to the properties.yaml file to be loaded
     * @return a new {@link Properties} object with a merged set of key/value properties
     * corresponding to the default and custom properties, global and for this instance-type
     * @throws IOException if the properties.yaml path cannot be read
     */
    public static Properties readConfigMap(@Nonnull final String componentType,
                                           @Nonnull final String propertiesYamlPath) throws IOException {
        final Properties result = CollectionFactory.createStringAdaptingProperties();
        final ConfigMapPropertiesReader propertiesReader = new ConfigMapPropertiesReader();
        final Resource propertiesYamlResource = new DefaultResourceLoader()
            .getResource(propertiesYamlPath);
        final Path filePath = Paths.get(propertiesYamlResource.getFile().getPath());

        if (!Files.exists(filePath)) {
            throw new FileNotFoundException("Missing file" + filePath);
        }
        propertiesReader.setResources(propertiesYamlResource);
        try {
            propertiesReader.process((properties, map) ->
                result.putAll(propertiesReader.extractProperties(componentType, map)));
        } catch (MapValueExpected | IllegalStateException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException)e.getCause();
            }
            throw e;
        }
        return result;
    }

    /**
     * Extract the global and component-type specific properties from the properties.yaml map
     * and combine them into a resulting Properties object.
     *
     * @param componentType the component-type for which the properties will be extracted
     * @param propertiesYamlMap the Map deserialized from properties.yaml
     * @return a Properties object containing the global and component-type specific properties
     * @throws MapValueExpected if the Yaml configuration file does not have a Map value
     * corresponding any of the {@link #PROP_SOURCE_SUBKEYS} values
     */
    private Properties extractProperties(@Nonnull final String componentType,
                                         @Nonnull final Map<String, Object> propertiesYamlMap
    ) throws MapValueExpected {
        final Properties result = new Properties();
        for (String segmentKey : PROP_SOURCE_SUBKEYS) {
            final Map<String, Object> propertiesMap = getSubMap(propertiesYamlMap, segmentKey);
            // include the null-check for the input parameter here instead of in every caller
            if (propertiesMap != null) {
                final Map<String, Object> globalDefaults = getSubMap(propertiesMap, "global");
                if (globalDefaults != null) {
                    logger.trace("{}: global defaults = {}", segmentKey, globalDefaults);
                    // convert all values to String
                    getFlattenedMap(globalDefaults).forEach((key, value) ->
                        result.put(key, value.toString()));
                }
                final Map<String, Object> componentDefaults = getSubMap(propertiesMap,
                    componentType);
                if (componentDefaults != null) {
                    logger.trace("{}: component defaults for {} = {}", segmentKey, componentType,
                        componentDefaults);
                    getFlattenedMap(componentDefaults).forEach((key, value) ->
                        result.put(key, value.toString()));
                } else {
                    logger.info("{}: no component defaults for: {}", segmentKey, componentType);
                }
            }
        }
        return result;
    }

    /**
     * Method to (a) extract the desired submap from the deserialized properties.yaml
     * configuration file, or null if there is no submap for the given propertiesKey, and
     * (b) verify the result is a map, and then cast as a {@code Map<String, Object>}
     * since this is the expected format of the deserialized properties.yaml.
     *
     * @param propertiesYamlMap the map object deserialized from the properties.yaml
     * @param propertiesKey the portion of the map to return, e.g. "global" or "component",
     *                      or a string representing a component type, e.g. "topology-processor"
     * @return the submap extracted from the properties.yaml map, or null if there is no
     * submap for the given propertiesKey
     * @throws MapValueExpected if the properties.yaml map for the given top-level section
     * key is not a map
     */
    @Nullable
    private static Map<String, Object> getSubMap(@Nonnull final Map<String, Object> propertiesYamlMap,
                                          @Nonnull final String propertiesKey
    ) throws MapValueExpected {
        final Object subMap = propertiesYamlMap.get(propertiesKey);
        if (subMap == null) {
            return null;
        }
        if (!(subMap instanceof Map)) {
            throw new MapValueExpected(
                    "Map<String, Object> for key " + propertiesKey + " was expected, not: "
                            + subMap.getClass().getCanonicalName() + "=" + subMap);
        }
        @SuppressWarnings("unchecked")
        final Map<String, Object> answer = (Map<String, Object>)subMap;
        return answer;
    }

    /**
     * Thrown when the YAML configuration map doesn't not have a Map value for one of the
     * major {@link #PROP_SOURCE_SUBKEYS}.
     */
    @VisibleForTesting
    static class MapValueExpected extends RuntimeException {
        MapValueExpected(String message) {
            super(message);
        }
    }
}
