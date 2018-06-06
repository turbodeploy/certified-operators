package com.vmturbo.group.setting;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpecCollection;

/**
 * Settings specifications store implementation, reading data from JSON file.
 */
public class FileBasedSettingsSpecStore implements SettingSpecStore {

    private final Logger logger = LogManager.getLogger(getClass());

    private final Map<String, SettingSpec> settingSpecMap;

    public FileBasedSettingsSpecStore(@Nonnull String path) {
        this.settingSpecMap = ImmutableMap.copyOf(parseSettingSpecJsonFile(path));
    }

    @Nonnull
    @Override
    public Optional<SettingSpec> getSettingSpec(@Nonnull String name) {
        return Optional.ofNullable(settingSpecMap.get(name));
    }

    /**
     * Parses the json config file, in order to deserialize {@link SettingSpec} objects, and add
     * them to the store.
     *
     * @param settingSpecJsonFile file to parse, containing the setting spec definition
     * @return collection of {@link SettingSpec} objects loaded from the file, indexed by name
     */
    @Nonnull
    private Map<String, SettingSpec> parseSettingSpecJsonFile(
            @Nonnull final String settingSpecJsonFile) {
        logger.debug("Loading setting spec json file: {}", settingSpecJsonFile);
        SettingSpecCollection.Builder collectionBuilder = SettingSpecCollection.newBuilder();

        // open the file and create a reader for it
        try (InputStream inputStream = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(settingSpecJsonFile);
             InputStreamReader reader = new InputStreamReader(inputStream)) {

            // parse the json file
            JsonFormat.parser().merge(reader, collectionBuilder);
        } catch (IOException e) {
            logger.error("Unable to load SettingSpecs from Json file: {}", settingSpecJsonFile, e);
        }

        final SettingSpecCollection settingSpecCollection = collectionBuilder.build();

        return settingSpecCollection.getSettingSpecsList()
                .stream()
                .collect(Collectors.toMap(SettingSpec::getName, Function.identity()));
    }

    /**
     * Gets all the {@link SettingSpec}.
     *
     * @return a collection of {@link SettingSpec}
     */
    @Nonnull
    @Override
    public Collection<SettingSpec> getAllSettingSpecs() {
        return settingSpecMap.values();
    }


    /**
     * Gets all the Global SettingSpecs.
     *
     * @return a collection of {@link SettingSpec}
     */
    @Nonnull
    @Override
    public Collection<SettingSpec> getAllGlobalSettingSpecs() {
        return settingSpecMap
                .values()
                .stream()
                .filter(SettingSpec::hasGlobalSettingSpec)
                .collect(Collectors.toList());
    }
}
