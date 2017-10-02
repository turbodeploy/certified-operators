package com.vmturbo.group.persistent;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.setting.SettingOuterClass.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.SettingSpecCollection;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.SettingSpecCollection.Builder;

/**
 * The {@link SettingStore} class is used to store settings-related objects, and retrieve them
 * in an efficient way.
 */
public class SettingStore {

    private final Logger logger = LogManager.getLogger();

    // map to store the setting spec, indexed by setting name
    private final ImmutableMap<String, SettingSpec> settingSpecMap;

    // json config file, containing the serialized instances of setting spec to load in memory
    private final String SETTING_SPEC_JSON_FILENAME;


    public SettingStore(final String settingSpecJsonFile) {
        this.SETTING_SPEC_JSON_FILENAME = settingSpecJsonFile;

        // read the json config file, and create setting spec instances in memory
        Map<String, SettingSpec> loadedMap = parseSettingSpecJsonFile(settingSpecJsonFile);

        settingSpecMap = ImmutableMap.<String, SettingSpec>builder().putAll(loadedMap).build();

    }

    /**
     * Parses the json config file, in order to deserialize {@link SettingSpec} objects, and add
     * them to the store.
     *
     * @param settingSpecJsonFile file to parse, containing the setting spec definition
     * @return collection of {@link SettingSpec} objects loaded from the file, indexed by name
     */
    private Map<String, SettingSpec> parseSettingSpecJsonFile(final String settingSpecJsonFile) {

        Builder collectionBuilder = SettingSpecCollection.newBuilder();

        // open the file and create a reader for it
        try (InputStream inputStream = Thread.currentThread()
                     .getContextClassLoader().getResourceAsStream(settingSpecJsonFile);
             InputStreamReader reader = new InputStreamReader(inputStream);
             ) {

            // parse the json file
            JsonFormat.parser().merge(reader, collectionBuilder);

        } catch (IOException e) {
            logger.error("Unable to load SettingSpecs from Json file: {}", settingSpecJsonFile, e);
        }

        SettingSpecCollection settingSpecCollection = collectionBuilder.build();
        List<SettingSpec> settingSpecsList = settingSpecCollection.getSettingSpecsList();

        // convert set to a map...
        return settingSpecsList.stream()
                    .collect(Collectors.toMap(SettingSpec::getName, Function.identity()));

    }

    /**
     * Gets the {@link SettingSpec} by name.
     *
     * @param specName setting name
     * @return selected {@link SettingSpec}
     */
    public SettingSpec getSettingSpec(String specName) {
        return settingSpecMap.get(specName);
    }

    /**
     * Gets all the {@link SettingSpec}.
     *
     * @return a collection of {@link SettingSpec}
     */
    public Collection<SettingSpec> getAllSettingSpec() {
        return settingSpecMap.values();
    }


}
