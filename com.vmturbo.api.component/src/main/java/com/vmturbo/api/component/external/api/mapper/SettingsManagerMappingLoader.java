package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.gson.Gson;

import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.GsonPostProcessable;

/**
 * Responsible for loading the {@link SettingsManagerMapping}s from a file at system startup.
 *
 *
 * TODO (roman, Nov 27 2018): It may be a good idea to move the mappings into code instead of
 * load them from JSON. See: OM-27688
 */
public class SettingsManagerMappingLoader {

    private static final Gson GSON = ComponentGsonFactory.createGson();

    private static final Logger logger = LogManager.getLogger();

    private final SettingsManagerMapping mapping;

    public SettingsManagerMappingLoader(@Nonnull final String settingsMgrJsonFileName) throws IOException {
        this.mapping = loadManagerMappings(settingsMgrJsonFileName);
    }

    @Nonnull
    private SettingsManagerMapping loadManagerMappings(@Nonnull final String settingsMgrJsonFileName)
            throws IOException {
        logger.info("Loading Setting Manager Mappings from {}...", settingsMgrJsonFileName);
        try (InputStream inputStream = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(settingsMgrJsonFileName);
             InputStreamReader reader = new InputStreamReader(inputStream)) {
            final SettingsManagerMapping mapping = GSON.fromJson(reader, SettingsManagerMapping.class);
            logger.info("Successfully loaded Setting Manager Mappings:\n{}", GSON.toJson(mapping));
            return mapping;
        }
    }

    @Nonnull
    public SettingsManagerMapping getMapping() {
        return mapping;
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
     * this writing (Nov 23, 2017), the UI has hard-coded expectations based on the setting
     * managers defined in the legacy OpsMgr. If that goes away, we could just have a single
     * "fake" XL manager that owns all the settings.
     * <p>
     * There is a JSON file that gets loaded when constructing the SettingsMapper. That file
     * should contain all the SettingManager -> Setting mappings, as well as auxiliary information
     * about each manager. These will need to be hard-coded (and kept up to date) to match
     * what's in the legacy OpsMgr.
     */
    public static class SettingsManagerMapping implements GsonPostProcessable {

        /**
         * (manager uuid) -> Information about that manager.
         */
        private final Map<String, SettingsManagerInfo> managersByUuid;

        /**
         * A map to quickly look up the manager for a particular setting name.
         * Explicitly marked as "transient" because it's not part of the GSON
         * serialization, and is initialized as part
         * of {@link SettingsManagerMapping#postDeserialize()}.
         */
        private transient Map<String, String> settingToManager;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private SettingsManagerMapping() {
            managersByUuid = new HashMap<>();
            settingToManager = new HashMap<>();
        }

        /**
         * Constructor for use in testing.
         */
        @VisibleForTesting
        SettingsManagerMapping(@Nonnull final Map<String, SettingsManagerInfo> managersByUuid,
                               @Nonnull final Map<String, String> settingToManager) {
            this.managersByUuid = managersByUuid;
            this.settingToManager = settingToManager;
        }

        /**
         * Get the name of the manager that "manages" a particular setting.
         *
         * @param specName The name of the setting.
         * @return An optional containing the name of the manager that manages this setting.
         *         An empty optional if there is no matching manager.
         */
        @Nonnull
        public Optional<String> getManagerUuid(@Nonnull final String specName) {
            return Optional.ofNullable(settingToManager.get(specName));
        }

        /**
         * Get the {@link SettingsManagerInfo} for a manager that "manages" a particular setting.
         *
         * @param specName The name of the setting.
         * @return An optional containing the {@link SettingsManagerInfo} for the manager.
         *         An empty optional if there is no matching manager.
         */
        @Nonnull
        public Optional<SettingsManagerInfo> getManagerForSetting(@Nonnull final String specName) {
            return Optional.ofNullable(settingToManager.get(specName))
                .flatMap(this::getManagerInfo);
        }

        /**
         * Get information about a manager by it's UUID.
         *
         * @param mgrUuid The UUID of the manager.
         * @return An optional containing the {@link SettingsManagerInfo} for the manager.
         *         An empty optional if the UUID is not found.
         */
        @Nonnull
        public Optional<SettingsManagerInfo> getManagerInfo(@Nonnull final String mgrUuid) {
            return Optional.ofNullable(managersByUuid.get(mgrUuid));
        }

        /**
         * Initialize the index of (setting name) -> (mgr uuid) after GSON deserialization
         * of the {@link SettingsManagerMapping#managersByUuid} map.
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

        /**
         * Convert "regular" setting specs into settings applicable in the plan UI.
         * See {@link PlanSettingInfo}.
         *
         * @param settingMgrs A collection of setting specs for realtime.
         * @return The setting specs from regularSettings that apply to plans.
         */
        @Nonnull
        public List<SettingsManagerApiDTO> convertToPlanSettingSpecs(
                @Nonnull final List<SettingsManagerApiDTO> settingMgrs) {
            final ImmutableList.Builder<SettingsManagerApiDTO> retBuilder = ImmutableList.builder();
            settingMgrs.forEach(settingMgr ->
                getManagerInfo(settingMgr.getUuid()).ifPresent(mgrInfo -> {
                    final SettingsManagerApiDTO newMgr = mgrInfo.newApiDTO(settingMgr.getUuid());
                    newMgr.setSettings(mgrInfo.getPlanSettingInfo()
                        .map(planSettingInfo -> settingMgr.getSettings().stream()
                            // When converting setting specs, we only want to display
                            // settings that have plan-specific conversions.
                            .map(planSettingInfo::toPlanSetting)
                            .filter(Optional::isPresent).map(Optional::get)
                            .collect(Collectors.toList()))
                        .orElse(Collections.emptyList()));
                    retBuilder.add(newMgr);
            }));
            return retBuilder.build();
        }

        /**
         * Convert "regular" setting values into settings applicable in the plan UI.
         * See {@link PlanSettingInfo}.
         *
         * @param realtimeSettings A collection of setting values in realtime terms.
         * @return The settings from regularSettings that can be displayed in the plan UI,
         *         with the plan-specific values.
         */
        @Nonnull
        public List<SettingApiDTO> convertToPlanSetting(
                @Nonnull final List<SettingApiDTO> realtimeSettings) {
            final ImmutableList.Builder<SettingApiDTO> retBuilder = ImmutableList.builder();
            realtimeSettings.forEach(realtimeSetting ->
                getManagerForSetting(realtimeSetting.getUuid()).ifPresent(mgrInfo ->
                    retBuilder.add(mgrInfo.getPlanSettingInfo()
                        .flatMap(planSettingInfo -> planSettingInfo.toPlanSetting(realtimeSetting))
                        .orElse(realtimeSetting))));
            return retBuilder.build();
        }

        /**
         * Convert settings values set in the plan UI, generated from the specs returned by
         * {@link SettingsManagerMapping#convertToPlanSettingSpecs(List)}, into settings parseable
         * in the rest of the system.
         *
         * See {@link PlanSettingInfo}.
         *
         * @param planSettings A collection of setting values from the plan UI.
         * @return The settings from planSettings that can be used in the rest of the system.
         */
        @Nonnull
        public List<SettingApiDTO> convertFromPlanSetting(
                @Nonnull final List<SettingApiDTO> planSettings) {
            final ImmutableList.Builder<SettingApiDTO> retBuilder = ImmutableList.builder();
            planSettings.forEach(planSetting ->
                getManagerForSetting(planSetting.getUuid()).ifPresent(mgrInfo ->
                    retBuilder.add(mgrInfo.getPlanSettingInfo()
                        .flatMap(planSettingInfo -> planSettingInfo.fromPlanSetting(planSetting))
                        .orElse(planSetting))));
            return retBuilder.build();
        }
    }

    /**
     * Some settings (most notably automation settings) are represented differently in the plan
     * view vs. in the settings view. The differences are:
     * 1) The entity types that support a particular setting are different in plans. For example,
     *    at the time of this writing VMs only support resize automation in plans (not move, suspend,
     *    or anything else).
     * 2) The setting values are different. For example, for Host suspend automation, the regular
     *    setting has several values (DISABLED, MANUAL, AUTOMATIC, etc) but in plans we only want
     *    to present two values - true (AUTOMATIC) and false (DISABLED).
     *
     * The {@link PlanSettingInfo} object is responsible for managing the conversion between "real"
     * and "plan" {@link SettingApiDTO}s.
     */
    public static class PlanSettingInfo {
        /**
         * UI-specific value <-> real setting value
         */
        private final BiMap<String, String> uiToXlValueConversion;

        /**
         * Entity type (num) -> setting name -> default value
         */
        private final Table<String, String, String> supportedSettingDefaults;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private PlanSettingInfo() {
            this.uiToXlValueConversion = HashBiMap.create();
            this.supportedSettingDefaults = HashBasedTable.create();
        }

        /**
         * Convert a {@link SettingApiDTO} representing a "real" setting in the system
         * to the {@link SettingApiDTO} representing the system for plans.
         *
         * @param realSetting The real setting, converted from a {@link SettingSpec}.
         * @return The {@link SettingApiDTO} to use for the plan UI. An empty optional if this
         *         setting has no mapping.
         */
        @Nonnull
        public Optional<SettingApiDTO> toPlanSetting(@Nonnull final SettingApiDTO realSetting) {
            final String defaultValue = supportedSettingDefaults.get(realSetting.getEntityType(),
                    realSetting.getUuid());
            if (defaultValue == null) {
                return Optional.empty();
            }

            final SettingApiDTO newDto = copySettingInfo(realSetting);
            newDto.setDefaultValue(defaultValue);

            // Fill in the value if it's present.
            if (realSetting.getValue() != null) {
                final String convertedValue = uiToXlValueConversion.inverse().get(realSetting.getValue());
                if (convertedValue == null) {
                    throw new IllegalArgumentException("Illegal value " + realSetting.getValue() +
                            " for setting " + realSetting.getUuid() + "meant to be converted to a" +
                            "plan setting! Legal values are: " +
                            StringUtils.join(uiToXlValueConversion.values(), ","));
                }
                newDto.setValue(convertedValue);
            }

            return Optional.of(newDto);
        }

        /**
         * Convert a {@link SettingApiDTO} representing a value set in a plan setting (generated via
         * {@link PlanSettingInfo#toPlanSetting(SettingApiDTO)}) to a "real" {@link SettingApiDTO}
         * that can be converted to a {@link Setting}.
         *
         * @param planSetting The plan setting, according to the model generated via
         *                    {@link PlanSettingInfo#toPlanSetting(SettingApiDTO)}.
         * @return A {@link SettingApiDTO} to use to convert to a {@link Setting}.
         *         This may be the input planSetting if the input was not created from a model
         *         generated by {@link PlanSettingInfo}.
         * @throws IllegalArgumentException If the plan setting is illegal - most notably
         *         if has a value that's not convertible.
         */
        @Nonnull
        public Optional<SettingApiDTO> fromPlanSetting(@Nonnull final SettingApiDTO planSetting)
                throws IllegalArgumentException {
            if (!supportedSettingDefaults.contains(planSetting.getEntityType(),
                    planSetting.getUuid())) {
                return Optional.empty();
            }

            final String convertedValue = uiToXlValueConversion.get(planSetting.getValue());
            if (convertedValue == null) {
                throw new IllegalArgumentException("Illegal value " + planSetting.getValue() +
                        " for plan setting " + planSetting.getUuid() + "! Legal values are: " +
                        StringUtils.join(uiToXlValueConversion.keySet(), ","));
            }

            final SettingApiDTO newDto = copySettingInfo(planSetting);
            newDto.setValue(convertedValue);
            return Optional.of(newDto);
        }

        /**
         * Creates a new {@link SettingApiDTO} that has the same information as an input
         * {@link SettingApiDTO} for all fields not affected by plan setting conversion.
         *
         * @param input The {@link SettingApiDTO} to copy.
         * @return A new {@link SettingApiDTO}.
         */
        @Nonnull
        private SettingApiDTO copySettingInfo(@Nonnull final SettingApiDTO input) {
            final SettingApiDTO newDto = new SettingApiDTO();
            newDto.setUuid(input.getUuid());
            newDto.setEntityType(input.getEntityType());
            newDto.setDisplayName(input.getDisplayName());
            return newDto;
        }
    }

    /**
     * The information about a specific Settings Manager. See {@link SettingsManagerMapping}.
     */
    public static class SettingsManagerInfo {
        /**
         * See {@link SettingsManagerInfo#getDisplayName()}.
         */
        private final String displayName;

        /**
         * See {@link SettingsManagerInfo#getDefaultCategory()}.
         */
        private final String defaultCategory;

        /**
         * See {@link SettingsManagerInfo#getSettings()}.
         */
        private final Set<String> settings;

        /**
         * See {@link SettingsManagerInfo#getPlanSettingInfo()}.
         */
        private final PlanSettingInfo planSettingInfo;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private SettingsManagerInfo() {
            displayName = "";
            defaultCategory = "";
            settings = new HashSet<>();
            planSettingInfo = new PlanSettingInfo();
        }

        /**
         * Explicit constructor for testing only.
         */
        @VisibleForTesting
        SettingsManagerInfo(@Nonnull final String displayName,
                            @Nonnull final String defaultCategory,
                            @Nonnull final Set<String> settings,
                            @Nonnull final PlanSettingInfo planSettingInfo) {
            this.displayName = displayName;
            this.defaultCategory = defaultCategory;
            this.settings = settings;
            this.planSettingInfo = planSettingInfo;
        }

        /**
         * @return the display name of the manager. It's not clear at the time of this writing that
         * the display name is explicitly used anywhere.
         */
        @Nonnull
        public String getDisplayName() {
            return displayName;
        }

        /**
         * @return the default category for all settings managed by this manager.
         * <p>
         * At the time of this writing the UI assigns this category to any setting managed by this
         * manager that does not have an explicit path
         * (i.e. {@link SettingApiDTO#getCategories()} returns null/empty).
         */
        @Nonnull
        public String getDefaultCategory() {
            return defaultCategory;
        }

        /**
         * @return the settings managed by this setting manager.
         * This must exactly match the name of some {@link SettingSpec}
         * ({@link SettingSpec#getName}).
         */
        @Nonnull
        public Set<String> getSettings() {
            return Collections.unmodifiableSet(settings);
        }

        /**
         * @return Information about conversions required to support the plan UI.
         * See {@link PlanSettingInfo}. Returns an empty optional if this manager
         * has no plan-related conversions.
         */
        @Nonnull
        public Optional<PlanSettingInfo> getPlanSettingInfo() {
            return Optional.ofNullable(planSettingInfo);
        }

        /**
         * Create a {@link SettingsManagerApiDTO} that represents the setting manager
         * this {@link SettingsManagerInfo} relates to.
         *
         * @param uuid The UUID of the setting manager. Because of the way we represent things in
         *             JSON, the {@link SettingsManagerInfo} doesn't know the UUID, so it has to get
         *             injected from outside. We can add a post-deserialize step in
         *             {@link SettingsManagerMapping} to inject UUIDs into {@link SettingsManagerInfo}
         *             if necessary.
         * @return A new {@link SettingsManagerApiDTO}, with no settings.
         */
        @Nonnull
        public SettingsManagerApiDTO newApiDTO(@Nonnull final String uuid) {
            final SettingsManagerApiDTO apiDTO = new SettingsManagerApiDTO();
            apiDTO.setUuid(uuid);
            apiDTO.setDisplayName(getDisplayName());
            apiDTO.setCategory(getDefaultCategory());
            return apiDTO;
        }
    }
}
