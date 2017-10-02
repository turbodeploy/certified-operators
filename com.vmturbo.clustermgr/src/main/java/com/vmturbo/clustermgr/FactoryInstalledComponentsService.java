package com.vmturbo.clustermgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Handler for FactoryInstalledComponents capturing the default components and configurations as
 * "shipped from the factory". Each component has a set of key/value pairs.
 *
 * The defaultPropertiesMap is populated from the resources/factoryInstalledComponents.yml file.
 * This file has a top-level entry for each component-type, whose value is a key/value map for default properties.
 **/
public class FactoryInstalledComponentsService {

    private Logger log = LogManager.getLogger();

    private static final String FACTORY_INSTALLED_COMPONENTS_FILE = "/factoryInstalledComponents.yml";

    private ComponentPropertiesMap defaultPropertiesMap = new ComponentPropertiesMap();

    /**
     * Read the {@link #FACTORY_INSTALLED_COMPONENTS_FILE} file when constructed, and save an
     * internal Map from component type to ComponentProperties.
     */
    public FactoryInstalledComponentsService() {
        Yaml yaml = new Yaml();

        try (InputStream s = getClass().getResourceAsStream(FACTORY_INSTALLED_COMPONENTS_FILE)){
            if (s == null) {
                throw new RuntimeException("New file resource is missing");
            }
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> factoryComponentsMap = (Map<String, Map<String, Object>>)yaml.load(s);
            log.debug("components: " + factoryComponentsMap.keySet());
            for(Map.Entry<String, Map<String, Object>> componentEntry : factoryComponentsMap.entrySet()) {
                log.info("componentEntry: " + componentEntry);
                ComponentProperties componentProps = new ComponentProperties();
                if (componentEntry.getValue() != null) {
                    // ensure that the property value is a string - the YAML import may yield different types (e.g. Integer)
                    for (Map.Entry<String,Object> rawPropertyValue :componentEntry.getValue().entrySet()) {
                        componentProps.put(rawPropertyValue.getKey(), rawPropertyValue.getValue().toString());
                    }
                }
                defaultPropertiesMap.addComponentConfiguration(componentEntry.getKey(),  componentProps);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + FACTORY_INSTALLED_COMPONENTS_FILE + " Not Found.", e);
        } catch (IOException e) {
            throw new RuntimeException("General I/O Exception reading " + FACTORY_INSTALLED_COMPONENTS_FILE, e);
        }

    }

    /**
     * Access the static Default Properties map from component_type to ComponentProperties.
     *
     * @return the Default Properties map from component_type to ComponentProperties.
     */
    ComponentPropertiesMap getFactoryInstalledComponents() {
        return defaultPropertiesMap;
    }

}
