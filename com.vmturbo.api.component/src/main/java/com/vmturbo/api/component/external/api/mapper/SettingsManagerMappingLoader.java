package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.service.SettingsService;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.GsonPostProcessable;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;

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

    private static final Set<ConfigurableActionSettings> virtualMachineResizeSettings =
        new HashSet<>(Arrays.asList(ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold,
            ConfigurableActionSettings.ResizeVcpuBelowMinThreshold,
            ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds,
            ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds,
            ConfigurableActionSettings.ResizeVmemAboveMaxThreshold,
            ConfigurableActionSettings.ResizeVmemBelowMinThreshold,
            ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds,
            ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds));

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
        @Nonnull
        private Map<String, SettingsManagerInfo> managersByUuid;

        /**
         * A map to quickly look up the manager for a particular setting name.
         * Explicitly marked as "transient" because it's not part of the GSON
         * serialization, and is initialized as part
         * of {@link SettingsManagerMapping#postDeserialize()}.
         */
        @Nonnull
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
         * Given a collection of manager names, get the set of setting names controlled by the managers. If
         * the collection of names is empty, then the return set will be empty too.
         *
         * <p>This will generate an {@link IllegalArgumentException} if an unrecognized manager id is requested.
         *
         * @param managerUuids the collection of manager uuids to find settings for.
         * @return the set of setting names owned by the input managers, or an empty set if no managers
         * were specified.
         */
        public Set<String> getSettingNamesForManagers(@Nullable Collection<String> managerUuids) {
            // get the list of setting names owned by te specified set of managers. If the manager uuid
            // collection is empty, then the return value will be empty too.
            return managerUuids == null
                    ? Collections.emptySet()
                    : managerUuids.stream()
                    .map(managerUuid -> {
                        Optional<SettingsManagerInfo> settingsManagerInfo = getManagerInfo(managerUuid);
                        if (!settingsManagerInfo.isPresent()) {
                            throw new IllegalArgumentException("Unsupported manager: " + managerUuid);
                        }
                        return settingsManagerInfo.get();
                    })
                    .map(SettingsManagerInfo::getSettings)
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
        }

        /**
         * Initialize the index of (setting name) -> (mgr uuid) after GSON deserialization
         * of the {@link SettingsManagerMapping#managersByUuid} map.
         */
        @Override
        public void postDeserialize() {
            // add generated sub settings for action mode
            final ImmutableMap.Builder<String, SettingsManagerInfo> managersByUuidBuilder =
                ImmutableMap.<String, SettingsManagerInfo>builder();
            managersByUuid.forEach((key, value) -> {
                // We do not add AUTOMATION_MANAGER nor CONTROL_MANAGER here because
                // we will add them later with the settings from settingManagers.json
                // and the generated settings from ActionSettingSpecs.
                // If we added them here, the immutable map builder would complain about
                // duplicate keys.
                if (!SettingsService.AUTOMATION_MANAGER.equals(key)
                        && !SettingsService.CONTROL_MANAGER.equals(key)) {
                    managersByUuidBuilder.put(key, value);
                }
            });

            // No need to add new settings to settingManagers.json.
            // only need to add it to ConfigurableActionSettings.
            // The execution schedule setting names are derived from ConfigurableActionSettings
            // in ActionSettingSpecs and ActionSettingType. No need to keep adding these
            // sub settings to settingsManagers.json. Keep all them in one place instead of keep
            // multiple files in sync.
            final Set<String> actionModeExecutionScheduleModeSettings =
                ActionSettingSpecs.getSettingSpecs().stream()
                    .map(SettingSpec::getName)
                    .filter(settingName -> ActionSettingSpecs.isExecutionScheduleSetting(settingName)
                        || ActionSettingSpecs.isActionModeSetting(settingName))
                    .collect(Collectors.toSet());
            replaceManagerSettings(SettingsService.AUTOMATION_MANAGER,
                actionModeExecutionScheduleModeSettings,
                managersByUuidBuilder);
            // Same comment as above. Keep all them in one place instead of keep multiple files
            // in sync.
            final Set<String> actionWorkflowSettings =
                ActionSettingSpecs.getSettingSpecs().stream()
                    .map(SettingSpec::getName)
                    .filter(ActionSettingSpecs::isActionWorkflowSetting)
                    .collect(Collectors.toSet());
            replaceManagerSettings(SettingsService.CONTROL_MANAGER,
                actionWorkflowSettings,
                managersByUuidBuilder);

            managersByUuid = managersByUuidBuilder.build();

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

        private void replaceManagerSettings(
                final String managerUuid,
                final Set<String> newSettings,
                ImmutableMap.Builder<String, SettingsManagerInfo> managersByUuidBuilder) {
            SettingsManagerInfo originalManagerInfo = managersByUuid.get(managerUuid);
            if (originalManagerInfo == null) {
                // This manager doesn't exist so we don't need to replace it. Some unit tests
                // don't provide a control manager even though the real file will always have a
                // control manager.
                return;
            }
            final Set<String> executionSettingIds = ImmutableSet.<String>builder()
                .addAll(originalManagerInfo.getSettings())
                .addAll(newSettings)
                .build();
            managersByUuidBuilder.put(managerUuid,
                new SettingsManagerInfo(
                    originalManagerInfo.getDisplayName(),
                    originalManagerInfo.getDefaultCategory(),
                    executionSettingIds,
                    originalManagerInfo.getPlanSettingInfo().orElseThrow(() ->
                        new IllegalArgumentException(
                            "settingManagers.json did not have a planSettingInfo"
                                + " for " + managerUuid))));
        }

        /**
         * Returns settings specs applicable in the plan UI.
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
                    mgrInfo.getPlanSettingInfo().ifPresent(planSettingInfo -> {
                        newMgr.setSettings(settingMgr.getSettings().stream()
                            .filter(planSettingInfo::isPlanRelevant)
                            .collect(Collectors.toList()));
                        retBuilder.add(newMgr);
                    });
                })
            );
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
        public List<SettingApiDTO<String>> convertToPlanSetting(
            @Nonnull final List<SettingApiDTO<String>> realtimeSettings) {
            final List<SettingApiDTO<String>> convertedSettings = new ArrayList<>();
            String resizeConvertedValue = null;
            Set<String> virtualMachineResizeSettingNames = virtualMachineResizeSettings
                .stream().map(ConfigurableActionSettings::getSettingName).collect(Collectors.toSet());
            for (SettingApiDTO realtimeSetting : realtimeSettings) {
                if (virtualMachineResizeSettingNames.contains(realtimeSetting.getUuid())
                    && realtimeSetting.getEntityType().equals(ApiEntityType.VIRTUAL_MACHINE.apiStr())) {
                    resizeConvertedValue = realtimeSetting.getValue().toString();
                } else {
                    convertedSettings.add(realtimeSetting);
                }
            }
            addPlanResizeSetting(convertedSettings, resizeConvertedValue);
            return convertedSettings;
        }


        /**
         * Convert settings values set in the plan UI, generated from the specs returned by
         * {@link SettingsManagerMapping#convertToPlanSettingSpecs(List)}, into settings parseable
         * in the rest of the system.
         *
         *<p>See {@link PlanSettingInfo}.
         *
         * @param planSettings A collection of setting values from the plan UI.
         * @return The settings from planSettings that can be used in the rest of the system.
         */
        @Nonnull
        public List<SettingApiDTO> convertFromPlanSetting(
                @Nonnull final List<SettingApiDTO<String>> planSettings) {
            final ImmutableList.Builder<SettingApiDTO> retBuilder = ImmutableList.builder();
            planSettings.forEach(planSetting ->
                getManagerForSetting(planSetting.getUuid()).ifPresent(mgrInfo ->
                    retBuilder.addAll(mgrInfo.getPlanSettingInfo()
                        .flatMap(planSettingInfo -> planSettingInfo.fromPlanSetting(planSetting))
                        .orElse(Arrays.asList(planSetting)))));
            return retBuilder.build();
        }

        /**
         * Add a Resize setting for the UI. This is needed since resize for vms doesn't exist as
         * an internal settings, but it maps to the settings defined in virtualMachineResizeSettings
         *
         * @param retBuilder Builder for a collection of setting.
         * @param resizeConvertedValue The converted value for the plan ui for the resize setting.
         */
        @VisibleForTesting
        void addPlanResizeSetting(List<SettingApiDTO<String>> retBuilder,
                                  String resizeConvertedValue) {
            if (resizeConvertedValue != null && getManagerForSetting(ConfigurableActionSettings.Resize.getSettingName()).isPresent()) {
                SettingsManagerInfo mgr =
                    getManagerForSetting(ConfigurableActionSettings.Resize.getSettingName()).get();
                if (mgr.getPlanSettingInfo().isPresent()) {
                    final SettingApiDTO resizeDto = new SettingApiDTO();
                    resizeDto.setUuid(ConfigurableActionSettings.Resize.getSettingName());
                    resizeDto.setEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
                    resizeDto.setDisplayName(ConfigurableActionSettings.Resize.getDisplayName());
                    resizeDto.setValue(resizeConvertedValue);
                    retBuilder.add(resizeDto);
                }
            }
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
         * Entity type (num) -> setting name -> default value
         */
        private final Table<String, String, String> supportedSettingDefaults;

        /**
         * Default constructor intentionally private. GSON constructs via reflection.
         */
        private PlanSettingInfo() {
            this.supportedSettingDefaults = HashBasedTable.create();
        }

        /**
         * Convert a {@link SettingApiDTO} representing a value set in a plan setting to a "real"
         * plan setting that is consistent with the backend values.
         *
         * @param planSetting The plan setting
         * @return A {@link SettingApiDTO} to use to convert to a List of {@link SettingApiDTO}.
         *         This may be the input planSetting if the input was not created from a model
         *         generated by {@link PlanSettingInfo}.
         * @throws IllegalArgumentException If the plan setting is illegal - most notably
         *         if has a value that's not convertible.
         */
        @Nonnull
        public Optional<List<SettingApiDTO>> fromPlanSetting(@Nonnull final SettingApiDTO planSetting)
            throws IllegalArgumentException {
            if (!supportedSettingDefaults.contains(planSetting.getEntityType(),
                planSetting.getUuid())) {
                return Optional.empty();
            }

            final String convertedValue = planSetting.getValue().toString();
            List<SettingApiDTO> newDtos = new ArrayList<>();
            if (ApiEntityType.fromString(planSetting.getEntityType()).equals(ApiEntityType.VIRTUAL_MACHINE)
                && planSetting.getUuid().equals(ConfigurableActionSettings.Resize.getSettingName())) {
                virtualMachineResizeSettings.forEach(resizeSetting -> {
                    final SettingApiDTO newResizeDto = new SettingApiDTO();
                    newResizeDto.setUuid(resizeSetting.getSettingName());
                    newResizeDto.setEntityType(planSetting.getEntityType());
                    newResizeDto.setDisplayName(resizeSetting.getDisplayName());
                    newResizeDto.setValue(convertedValue);
                    newDtos.add(newResizeDto);
                });
            } else {
                newDtos.add(planSetting);

            }
            return Optional.of(newDtos);
        }

         /**
         * Checks if setting is allowed in Plan UI.
         * Not all settings are supported and allowed to be configurable in Plans
         * and we filter out all not supported
         *
         * @param setting settingApiDto which is checked against those allowed in plan
         * @param <T> Value of realSetting
         * @return true if realSetting part of allow plan actions
         */
        public <T extends Serializable> boolean isPlanRelevant(@Nonnull final SettingApiDTO<T> setting) {
            return supportedSettingDefaults.get(setting.getEntityType(),
                    setting.getUuid()) != null;
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
         * Sort the input setting specs (or other objects that are associated with setting names)
         * according to the order in which the names appear in the settingManagers.json file.
         *
         * @param unordered The unordered objects (e.g. setting specs).
         * @param nameExtractor Function to extract the name from the object type.
         * @return An ordered list of the input objects according to the order specified in
         *         this manager's entry in settingManagers.json.
         */
        @Nonnull
        public <T> List<T> sortSettingSpecs(@Nonnull final Collection<T> unordered,
                                            @Nonnull final Function<T, String> nameExtractor) {
            Map<String, Integer> nameIndices = new HashMap<>();
            int i = 0;
            // The "settings" set is deserialized by GSON into a linked hash set, which preserves
            // the order from the settingManagers.json file.
            for (String name : settings) {
                nameIndices.put(name, i++);
            }

            // Sort according to setting names, because the method is called with the argument
            // SettingSpec::getName corresponding to the nameExtractor parameter
            final List<T> sortedSpecs = unordered.stream()
                .sorted(Comparator.comparingInt(spec -> nameIndices.getOrDefault(nameExtractor.apply(spec), -1)))
                .collect(Collectors.toList());
            return sortedSpecs;
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
