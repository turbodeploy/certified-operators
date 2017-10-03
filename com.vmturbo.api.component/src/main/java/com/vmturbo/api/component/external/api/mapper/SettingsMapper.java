package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingOptionApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.enums.SettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.ScopeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.GsonPostProcessable;

/**
 * Responsible for mapping Settings-related XL objects to their API counterparts.
 */
public class SettingsMapper {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A constant indicating that a {@link SettingSpec} has no associated setting manager.
     */
    private static final String NO_MANAGER = "";

    private final SettingManagerMapping managerMapping;

    private final SettingSpecMapper settingSpecMapper;

    public SettingsMapper(@Nonnull final String specJsonFile) {
        this.managerMapping = loadManagerMappings(specJsonFile);
        this.settingSpecMapper = new DefaultSettingSpecMapper();
    }

    @VisibleForTesting
    SettingsMapper(@Nonnull final SettingManagerMapping managerMapping,
                  @Nonnull final SettingSpecMapper specMapper) {
        this.managerMapping = managerMapping;
        this.settingSpecMapper = specMapper;
    }

    @Nonnull
    private SettingManagerMapping loadManagerMappings(@Nonnull final String specJsonFile) {
        logger.info("Loading Setting Manager Mappings from {}...", specJsonFile);
        try (InputStream inputStream = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(specJsonFile);
            InputStreamReader reader = new InputStreamReader(inputStream)) {
            final SettingManagerMapping mapping =
                    ComponentGsonFactory.createGson().fromJson(reader, SettingManagerMapping.class);
            logger.info("Successfully loaded Setting Manager Mappings.");
            return mapping;
        } catch (IOException e) {
            throw new RuntimeException("Unable to load setting manager mapping.", e);
        }
    }

    /**
     * Convert a collection of {@link SettingSpec} objects into the {@link SettingsManagerApiDTO}
     * that will represent these settings to the UI.
     *
     * @param specs The collection of specs.
     * @return
     */
    public List<SettingsManagerApiDTO> toManagerDtos(@Nonnull final Collection<SettingSpec> specs) {
        final Map<String, List<SettingSpec>> specsByMgr = specs.stream()
                .collect(Collectors.groupingBy(spec -> managerMapping.getManagerUuid(spec)
                        .orElse(NO_MANAGER)));

        final List<SettingSpec> unhandledSpecs = specsByMgr.get(NO_MANAGER);
        if (unhandledSpecs != null && !unhandledSpecs.isEmpty()) {
            final List<String> unhandledNames = unhandledSpecs.stream()
                    .map(SettingSpec::getName)
                    .collect(Collectors.toList());
            logger.warn("The following settings don't have a mapping in the API component." +
                    " Not returning them to the user. Settings: {}", unhandledNames);
        }

        return specsByMgr.entrySet().stream()
                // Don't return the specs that don't have a Manager mapping
                .filter(entry -> !entry.getKey().equals(NO_MANAGER))
                .map(entry -> {
                    final SettingManagerInfo info = managerMapping.getManagerInfo(entry.getKey())
                            .orElseThrow(() -> new IllegalStateException("Manager ID " +
                                    entry.getKey() + " not found despite being in the mappings earlier."));
                    return createMgrDto(entry.getKey(), info, entry.getValue());
                })
                .collect(Collectors.toList());
    }

    public Optional<SettingsManagerApiDTO> toManagerDto(
            @Nonnull final Collection<SettingSpec> specs,
            @Nonnull final String desiredMgrId) {
        final Optional<SettingManagerInfo> infoOpt = managerMapping.getManagerInfo(desiredMgrId);
        return infoOpt.map(info -> createMgrDto(desiredMgrId, info, specs.stream()
                .filter(spec -> managerMapping.getManagerUuid(spec)
                    .map(name -> name.equals(desiredMgrId))
                    .orElse(false))
                .collect(Collectors.toList())));

    }

    @Nonnull
    private SettingsManagerApiDTO createMgrDto(@Nonnull final String mgrId,
                                               @Nonnull final SettingManagerInfo info,
                                               @Nonnull final Collection<SettingSpec> specs) {
        final SettingsManagerApiDTO mgrApiDto = new SettingsManagerApiDTO();
        mgrApiDto.setUuid(mgrId);
        mgrApiDto.setDisplayName(info.getDisplayName());
        mgrApiDto.setCategory(info.getDefaultCategory());

        mgrApiDto.setSettings(specs.stream()
                .map(settingSpecMapper::settingSpecToApi)
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toList()));
        return mgrApiDto;
    }

    /**
     * The mapper from {@link SettingSpec} to {@link SettingApiDTO}, extracted as an interface
     * for unit testing/mocking purposes.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface SettingSpecMapper {
        Optional<SettingApiDTO> settingSpecToApi(@Nonnull final SettingSpec settingSpec);
    }

    /**
     * The "real" implementation of {@link SettingSpecMapper}.
     */
    @VisibleForTesting
    static class DefaultSettingSpecMapper implements SettingSpecMapper {

        @Override
        public Optional<SettingApiDTO> settingSpecToApi(@Nonnull final SettingSpec settingSpec) {
            final SettingApiDTO apiDTO = new SettingApiDTO();
            apiDTO.setUuid(settingSpec.getName());
            apiDTO.setDisplayName(settingSpec.getDisplayName());

            if (settingSpec.hasPath()) {
                final List<String> categories = new LinkedList<>();
                SettingCategoryPathNode pathNode = settingSpec.getPath().getRootPathNode();
                categories.add(pathNode.getNodeName());
                while (pathNode.hasChildNode()) {
                    pathNode = pathNode.getChildNode();
                    categories.add(pathNode.getNodeName());
                }
                apiDTO.setCategories(categories);
            }

            switch (settingSpec.getSettingTypeCase()) {
                case ENTITY_SETTING_SPEC:
                    apiDTO.setScope(SettingScope.LOCAL);
                    final EntitySettingScope entityScope =
                            settingSpec.getEntitySettingSpec().getEntitySettingScope();
                    if (entityScope.getScopeCase().equals(ScopeCase.ENTITY_TYPE_SET)) {
                        // TODO (roman, Sept 12, 2017): Right now there are no settings that
                        // have multiple entity types, so we can just print a warning if we
                        // see one. In the future, either the UI API should change, or we should
                        // create a list of SettingApiDTOs when a SettingSpec contains multiple
                        // types.
                        final EntityTypeSet supportedTypes = entityScope.getEntityTypeSet();
                        if (!supportedTypes.getEntityTypeList().isEmpty()) {
                            if (supportedTypes.getEntityTypeCount() > 1) {
                                logger.warn("The spec {} supports multiple entity types: {}." +
                                                " Using only the first one.", settingSpec.getName(),
                                        supportedTypes.getEntityTypeList());
                            }
                            apiDTO.setEntityType(
                                ServiceEntityMapper.toUIEntityType(supportedTypes.getEntityType(0)));
                        }
                    }
                    break;
                case GLOBAL_SETTING_SPEC:
                    apiDTO.setScope(SettingScope.GLOBAL);
                    break;
            }

            switch (settingSpec.getSettingValueTypeCase()) {
                case BOOLEAN_SETTING_VALUE_TYPE:
                    apiDTO.setValueType(InputValueType.BOOLEAN);
                    apiDTO.setDefaultValue(
                            Boolean.toString(settingSpec.getBooleanSettingValueType().getDefault()));
                    break;
                case NUMERIC_SETTING_VALUE_TYPE:
                    apiDTO.setValueType(InputValueType.NUMERIC);
                    final NumericSettingValueType numericType = settingSpec.getNumericSettingValueType();
                    apiDTO.setDefaultValue(Float.toString(numericType.getDefault()));
                    if (numericType.hasMin()) {
                        apiDTO.setMin((double) numericType.getMin());
                    }
                    if (numericType.hasMax()) {
                        apiDTO.setMax((double) numericType.getMax());
                    }
                    break;
                case STRING_SETTING_VALUE_TYPE:
                    apiDTO.setValueType(InputValueType.STRING);
                    final StringSettingValueType stringType = settingSpec.getStringSettingValueType();
                    apiDTO.setDefaultValue(stringType.getDefault());
                    break;
                case ENUM_SETTING_VALUE_TYPE:
                    // Enum is basically a string with predefined allowable values.
                    apiDTO.setValueType(InputValueType.STRING);
                    final EnumSettingValueType enumType = settingSpec.getEnumSettingValueType();
                    apiDTO.setDefaultValue(enumType.getDefault());
                    apiDTO.setOptions(enumType.getEnumValuesList().stream()
                            .map(enumValue -> {
                                final SettingOptionApiDTO enumOption = new SettingOptionApiDTO();
                                enumOption.setLabel(enumValue);
                                enumOption.setValue(enumValue);
                                return enumOption;
                            })
                            .collect(Collectors.toList()));
                    break;
            }

            return Optional.of(apiDTO);
        }
    }

    /**
     * The Java POJO representing the mappings from Setting Manager UUIDs, to the information about
     * that Setting Manager - most notably the settings it manages.
     * <p>
     * The UI/API has the concept of Setting Manager as an object that "manages" a group of settings.
     * This is inherited from legacy, where these various managers are EMF objects.
     * In XL we don't have setting managers. We just have {@link SettingSpec}s for all the available
     * settings. Instead of introducing the manager concept to the data model in XL, we fake it
     * at the API-component level. The reason we need to simulate it is because, at the time of
     * this writing (Sept 13, 2017), the UI has hard-coded expectations based on the setting
     * managers defined in the legacy OpsMgr. If that goes away, we could just have a single
     * "fake" XL manager that owns all the settings.
     * <p>
     * There is a JSON file that gets loaded when constructing the SettingsMapper. That file
     * should contain all the SettingManager -> Setting mappings, as well as auxiliary information
     * about each manager. These will need to be hard-coded (and kept up to date) to match
     * what's in the legacy OpsMgr.
     */
    @VisibleForTesting
    static class SettingManagerMapping implements GsonPostProcessable {

        /**
         * (manager uuid) -> Information about that manager.
         */
        private final Map<String, SettingManagerInfo> managersByUuid;

        /**
         * A map to quickly look up the manager for a particular setting name.
         * Explicitly marked as "transient" because it's not part of the GSON
         * serialization, and is initialized as part
         * of {@link SettingManagerMapping#postDeserialize()}.
         */
        private transient Map<String, String> settingToManager = new HashMap<>();

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private SettingManagerMapping() {
            managersByUuid = new HashMap<>();
        }

        /**
         * Get the name of the manager that "manages" a particular setting.
         *
         * @param spec The {@link SettingSpec} describing the setting.
         * @return An optional containing the name of the manager that manages this setting.
         *         An empty optional if there is no matching manager.
         */
        Optional<String> getManagerUuid(@Nonnull final SettingSpec spec) {
            return Optional.ofNullable(settingToManager.get(spec.getName()));
        }

        /**
         * Get information about a manager by it's UUID.
         *
         * @param mgrUuid The UUID of the manager.
         * @return An optional containing the {@link SettingManagerInfo} for the manager.
         *         An empty optional if the UUID is not found.
         */
        Optional<SettingManagerInfo> getManagerInfo(@Nonnull final String mgrUuid) {
            return Optional.of(managersByUuid.get(mgrUuid));
        }

        /**
         * Initialize the index of (setting name) -> (mgr uuid) after GSON deserialization
         * of the {@link SettingManagerMapping#managersByUuid} map.
         */
        @Override
        public void postDeserialize() {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            managersByUuid.forEach((mgrName, mgrInfo) -> {
                mgrInfo.getSettings().forEach((settingName) -> {
                    // At the time of this writing (Sept 2017) settings names must be
                    // globally unique.
                    builder.put(settingName, mgrName);
                });
            });
            settingToManager = builder.build();
        }
    }

    /**
     * The information about a specific Setting Manager. See {@link SettingManagerMapping}.
     */
    @VisibleForTesting
    static class SettingManagerInfo {
        /**
         * The display name of the manager. It's not clear at the time of this writing that
         * the display name is explicitly used anywhere.
         */
        private final String displayName;

        /**
         * This represents the default category for all settings managed by this manager.
         * <p>
         * At the time of this writing the UI assigns this category to any setting managed by this
         * manager that does not have an explicit path
         * (i.e. {@link SettingApiDTO#getCategories()} returns null/empty).
         */
        private final String defaultCategory;

        /**
         * The settings managed by this setting manager.
         * This must exactly match the name of some {@link SettingSpec}
         * ({@link SettingSpec#getName}).
         */
        private final Set<String> settings;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private SettingManagerInfo() {
            displayName = "";
            defaultCategory = "";
            settings = new HashSet<>();
        }

        /**
         * Explicit constructor for testing only.
         */
        @VisibleForTesting
        SettingManagerInfo(@Nonnull final String displayName,
                @Nonnull final String defaultCategory,
                @Nonnull final Set<String> settings) {
           this.displayName = displayName;
           this.defaultCategory = defaultCategory;
           this.settings = settings;
        }

        String getDisplayName() {
            return displayName;
        }

        String getDefaultCategory() {
            return defaultCategory;
        }

        Set<String> getSettings() {
            return Collections.unmodifiableSet(settings);
        }
    }

}
