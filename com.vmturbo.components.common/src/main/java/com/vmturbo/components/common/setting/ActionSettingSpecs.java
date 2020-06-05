package com.vmturbo.components.common.setting;

import static com.vmturbo.components.common.setting.SettingDTOUtil.createSettingCategoryPath;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType.Type;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Each action mode comes with 4 settings. To ensure we do not miss any and to prevent typos, we
 * programmatically generate the 4 settings for each action mode setting. This protects against:
 * <ul>
 *     <li>Forgetting to add the these 4 settings when a new action mode setting is added.</li>
 *     <li>Copying and pasting errors when modifying the displayName or settingName</li>
 * </ul>
 */
public class ActionSettingSpecs {

    private static final String ACTION_WORKFLOW_SETTING_NAME_SUFFIX = "ActionWorkflow";
    private static final String EXTERNAL_APPROVAL_SETTING_NAME_PREFIX = "approval";
    private static final String ON_GENERATION_SETTING_NAME_PREFIX = "onGen";
    private static final String AFTER_EXECUTION_SETTING_NAME_PREFIX = "afterExec";
    private static final String EXECUTION_SCHEDULE_SETTING_NAME_SUFFIX = "ExecutionSchedule";
    private static final String EXTERNAL_APPROVAL_DISPLAY_NAME_PREFIX =
        "External Approval Workflow for a ";
    private static final String ACTION_DISPLAY_NAME_SUFFIX = " action";
    private static final String EXTERNAL_AUDIT_DISPLAY_NAME_PREFFIX =
        "Workflow to run when we ";
    private static final String ON_GENERATION_DISPLAY_NAME_PREFFIX = "generate a ";
    private static final String AFTER_EXECUTION_DISPLAY_NAME_PREFFIX = "complete or fail a ";
    private static final String EXECUTION_SCHEDULE_DISPLAY_NAME_PREFIX =  "Execution window for ";

    /**
     * Contains an ActionSettingSpecs for each action mode setting in {@link EntitySettingSpecs}.
     */
    @Nonnull
    private static final List<ActionSettingSpecs> ACTION_SETTING_SPECS;

    /**
     * All the {@link SettingSpec}s generated from the {@link ActionSettingSpecs}. Each
     * ActionSettingSpecs will have 4 SettingSpecs (External Approval, on generation audit,
     * after exec audit, and execution schedule). As a result, this size of this list will be
     * 4 * size ofACTION_SETTING_SPECS.
     */
    @Nonnull
    private static final List<SettingSpec> SETTING_SPECS;

    /**
     * A map of each setting name to it's SettingSpec. There will be one key value in this map for
     * each entry in SETTING_SPECS.
     */
    @Nonnull
    private static final Map<String, SettingSpec> SETTING_NAME_TO_SETTING_SPEC;

    private static final StringSettingDataType WORKFLOW_TYPE =
        new StringSettingDataType(
            EntitySettingSpecs.DEFAULT_STRING_VALUE,
            EntitySettingSpecs.MATCH_ANYTHING_REGEX);

    /**
     * Contains information about ActionMode setting and corresponding ExecutionSchedule setting
     * that can be used optionally in settings policies.
     * Execution schedule windows define when action could be executed.
     */
    @Nonnull
    private static final BiMap<String, String> ACTION_MODE_TO_EXECUTION_SCHEDULE_SETTINGS;

    static {
        ACTION_SETTING_SPECS = Arrays.stream(EntitySettingSpecs.values())
            .filter(entitySettingSpecs ->
                entitySettingSpecs.getDataStructure() instanceof EnumSettingDataType)
            .filter(entitySettingSpecs ->
                ActionMode.class.equals(
                    ((EnumSettingDataType)entitySettingSpecs.getDataStructure()).getEnumClass()))
            .map(entitySettingSpecs -> new ActionSettingSpecs(entitySettingSpecs))
            .collect(Collectors.toList());

        SETTING_SPECS = ACTION_SETTING_SPECS.stream()
            .map(ActionSettingSpecs::createSettingSpecs)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        SETTING_NAME_TO_SETTING_SPEC = SETTING_SPECS.stream()
            .collect(Collectors.toMap(SettingSpec::getName, Function.identity()));

        final ImmutableBiMap.Builder<String, String> actionModeToExecutionScheduleBuilder =
            new ImmutableBiMap.Builder<>();
        ACTION_SETTING_SPECS.forEach(actionSettingSpec ->
            actionModeToExecutionScheduleBuilder.put(
                actionSettingSpec.actionModeSettingSpec.getSettingName(),
                actionSettingSpec.getExecutionScheduleSettingName()));
        ACTION_MODE_TO_EXECUTION_SCHEDULE_SETTINGS = actionModeToExecutionScheduleBuilder.build();
    }

    @Nonnull
    private final EntitySettingSpecs actionModeSettingSpec;

    /**
     * Get all the action related settings as {@link SettingSpec} objects.
     *
     * @return all the action related settings as {@link SettingSpec} objects.
     */
    @Nonnull
    public static List<SettingSpec> getSettingSpecs() {
        return SETTING_SPECS;
    }

    /**
     * Get the {@link SettingSpec} with the provided settingName.
     *
     * @param settingName the settingName to search using.
     * @return the {@link SettingSpec} with the provided settingName.
     */
    @Nullable
    public static SettingSpec getSettingSpec(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return SETTING_NAME_TO_SETTING_SPEC.get(settingName);
    }

    /**
     * Determines if the provided settingName is an action related setting.
     *
     * @param settingName the settingName to check.
     * @return true of the given settingName is an action related setting.
     */
    public static boolean isActionModeSubSetting(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return SETTING_NAME_TO_SETTING_SPEC.containsKey(settingName);
    }

    /**
     * Determines if the provided settingName is related to External Approval or Audit.
     *
     * @param settingName the settingName to check.
     * @return true if the provided settingName is related to External Approval or Audit.
     */
    public static boolean isExternalApprovalOrAuditSetting(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return SETTING_NAME_TO_SETTING_SPEC.containsKey(settingName)
            && !isExecutionScheduleSetting(settingName);
    }

    /**
     * Determines if the provided settingName is related to Execution Schedules.
     *
     * @param settingName the settingName to check.
     * @return true if the provided settingName is related to Execution Schedules.
     */
    public static boolean isExecutionScheduleSetting(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return ACTION_MODE_TO_EXECUTION_SCHEDULE_SETTINGS.containsValue(settingName);
    }

    /**
     * Returns the action mode setting name for the provided execution schedule setting name.
     *
     * @param executionSetting the execution schedule setting name to search for.
     * @return the action mode setting name for the provided execution schedule setting name.
     *         Returns null if not found.
     */
    @Nullable
    public static String getActionModeSettingFromExecutionScheduleSetting(
            @Nonnull String executionSetting) {
        Objects.requireNonNull(executionSetting);
        return ACTION_MODE_TO_EXECUTION_SCHEDULE_SETTINGS.inverse().get(executionSetting);
    }

    /**
     * Returns the execution schedule setting name for the provided action mode setting name.
     *
     * @param actionModeSettingName action mode setting name to search for.
     * @return the execution schedule setting name for the provided action mode setting name.
     *         Returns null if not found.
     */
    @Nullable
    public static String getExecutionScheduleSettingFromActionModeSetting(
        @Nonnull String actionModeSettingName) {
        Objects.requireNonNull(actionModeSettingName);
        return ACTION_MODE_TO_EXECUTION_SCHEDULE_SETTINGS.get(actionModeSettingName);
    }

    /**
     * Returns the execution schedule setting name for the provided entitySettingSpecs.
     *
     * @param entitySettingSpecs entitySettingSpecs to search for.
     * @return the execution schedule setting name for the provided entitySettingSpecs.
     *         Returns null if not found.
     */
    @Nullable
    public static String getExecutionScheduleSettingFromActionModeSetting(
            @Nonnull EntitySettingSpecs entitySettingSpecs) {
        Objects.requireNonNull(entitySettingSpecs);
        return ACTION_MODE_TO_EXECUTION_SCHEDULE_SETTINGS.get(entitySettingSpecs.getSettingName());
    }

    /**
     * Determines if the provided setting name is for a setting that sets the action mode.
     *
     * @param settingName the setting name to search using.
     * @return true if the provided setting name is for the setting that sets the action mode.
     */
    public static boolean isActionModeSetting(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return ACTION_MODE_TO_EXECUTION_SCHEDULE_SETTINGS.containsKey(settingName);
    }

    /**
     * Determines if the provided entitySettingSpecs is for a setting that sets the action mode.
     *
     * @param entitySettingSpecs the entitySettingSpecs to search using.
     * @return true if the provided entitySettingSpecs is for the setting that sets the action mode.
     */
    public static boolean isActionModeSetting(@Nonnull EntitySettingSpecs entitySettingSpecs) {
        Objects.requireNonNull(entitySettingSpecs);
        return ACTION_MODE_TO_EXECUTION_SCHEDULE_SETTINGS.containsKey(
            entitySettingSpecs.getSettingName());
    }

    /**
     * Constructs an ActionSettingSpecs based on the action mode setting.
     *
     * @param actionModeSettingSpec the action mode setting to generate more settings for.
     */
    public ActionSettingSpecs(final @Nonnull EntitySettingSpecs actionModeSettingSpec) {
        Objects.requireNonNull(actionModeSettingSpec);
        this.actionModeSettingSpec = actionModeSettingSpec;
    }

    @Nonnull
    private List<SettingSpec> createSettingSpecs() {
        return Arrays.asList(
            createActionWorkflowSettingSpec(
                getExternalApprovalTypeSettingName(),
                getExternalApprovalTypeDescription()),
            createActionWorkflowSettingSpec(
                getOnGenExternalAuditTypeSettingName(),
                getOnGenExternalAuditTypeDescription()),
            createActionWorkflowSettingSpec(
                getAfterExecExternalAuditTypeSettingName(),
                getAfterExecExternalAuditTypeDescription()),
            createOidSettingSpec(
                new SortedSetOfOidSettingDataType(Type.ENTITY, Collections.emptySet()),
                getExecutionScheduleSettingName(),
                getExecutionScheduleDescription())
        );
    }

    /**
     * Create the setting specification in the same way that {@link EntitySettingSpecs} creates
     * ActionWorkflow settings like {@link EntitySettingSpecs#PreActivateActionWorkflow}.
     *
     * @param name the setting name.
     * @param displayName the display name for the setting.
     * @return the setting specification describing an on generation or after execution audit
     *         workflow.
     */
    @Nonnull
    private SettingSpec createActionWorkflowSettingSpec(
        @Nonnull String name,
        @Nonnull String displayName) {
        return createSettingSpec(
            WORKFLOW_TYPE,
            Collections.singletonList(CategoryPathConstants.AUTOMATION),
            SettingTiebreaker.SMALLER, name, displayName);
    }

    @Nonnull
    private SettingSpec createOidSettingSpec(
        @Nonnull SortedSetOfOidSettingDataType dataStructure,
        @Nonnull String name,
        @Nonnull String displayName) {
        return createSettingSpec(dataStructure, SettingTiebreaker.UNION, name, displayName);
    }

    @Nonnull
    private SettingSpec createSettingSpec(
            @Nonnull AbstractSettingDataType<?> dataStructure,
            @Nonnull SettingTiebreaker settingTiebreaker,
            @Nonnull String name,
            @Nonnull String displayName) {
        return createSettingSpec(dataStructure, Collections.emptyList(), settingTiebreaker,
            name, displayName);
    }

    @Nonnull
    private SettingSpec createSettingSpec(
            @Nonnull AbstractSettingDataType<?> dataStructure,
            @Nonnull List<String> categoryPath,
            @Nonnull SettingTiebreaker settingTiebreaker,
            @Nonnull String name,
            @Nonnull String displayName) {
        final EntitySettingScope.Builder scopeBuilder = EntitySettingScope.newBuilder();
        if (actionModeSettingSpec.getEntityTypeScope().isEmpty()) {
            scopeBuilder.setAllEntityType(AllEntityType.getDefaultInstance());
        } else {
            scopeBuilder.setEntityTypeSet(EntityTypeSet.newBuilder()
                .addAllEntityType(actionModeSettingSpec.getEntityTypeScope().stream()
                    .map(EntityType::getNumber)
                    .collect(Collectors.toSet())));
        }
        final SettingSpec.Builder builder = SettingSpec.newBuilder()
            .setName(name)
            .setDisplayName(displayName)
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                .setTiebreaker(settingTiebreaker)
                .setEntitySettingScope(scopeBuilder)
                .setAllowGlobalDefault(actionModeSettingSpec.isAllowGlobalDefault()));
        if (!categoryPath.isEmpty()) {
            builder.setPath(createSettingCategoryPath(categoryPath));
        }
        dataStructure.build(builder);
        return builder.build();
    }

    @Nonnull
    private String getExternalApprovalTypeSettingName() {
        return EXTERNAL_APPROVAL_SETTING_NAME_PREFIX
            + StringUtils.capitalize(actionModeSettingSpec.getSettingName())
            + ACTION_WORKFLOW_SETTING_NAME_SUFFIX;
    }

    @Nonnull
    private String getOnGenExternalAuditTypeSettingName() {
        return ON_GENERATION_SETTING_NAME_PREFIX
            + StringUtils.capitalize(actionModeSettingSpec.getSettingName())
            + ACTION_WORKFLOW_SETTING_NAME_SUFFIX;
    }

    @Nonnull
    private String getAfterExecExternalAuditTypeSettingName() {
        return AFTER_EXECUTION_SETTING_NAME_PREFIX
            + StringUtils.capitalize(actionModeSettingSpec.getSettingName())
            + ACTION_WORKFLOW_SETTING_NAME_SUFFIX;
    }

    @Nonnull
    private String getExecutionScheduleSettingName() {
        return actionModeSettingSpec.getSettingName() + EXECUTION_SCHEDULE_SETTING_NAME_SUFFIX;
    }

    @Nonnull
    private String getExternalApprovalTypeDescription() {
        return EXTERNAL_APPROVAL_DISPLAY_NAME_PREFIX
            + actionModeSettingSpec.getDisplayName() + ACTION_DISPLAY_NAME_SUFFIX;
    }

    @Nonnull
    private String getOnGenExternalAuditTypeDescription() {
        return EXTERNAL_AUDIT_DISPLAY_NAME_PREFFIX + ON_GENERATION_DISPLAY_NAME_PREFFIX
            + actionModeSettingSpec.getDisplayName() + ACTION_DISPLAY_NAME_SUFFIX;
    }

    @Nonnull
    private String getAfterExecExternalAuditTypeDescription() {
        return EXTERNAL_AUDIT_DISPLAY_NAME_PREFFIX + AFTER_EXECUTION_DISPLAY_NAME_PREFFIX
            + actionModeSettingSpec.getDisplayName() + ACTION_DISPLAY_NAME_SUFFIX;
    }

    @Nonnull
    private String getExecutionScheduleDescription() {
        return EXECUTION_SCHEDULE_DISPLAY_NAME_PREFIX + actionModeSettingSpec.getDisplayName();
    }
}
