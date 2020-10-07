package com.vmturbo.components.common.setting;

import static com.vmturbo.components.common.setting.SettingDTOUtil.createSettingCategoryPath;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType.Type;
import com.vmturbo.common.protobuf.utils.ProbeFeature;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Enumeration of all the action specific settings. All they are created from the "base" setting,
 * which is a setting, holding action mode, i.e. {@link ConfigurableActionSettings#Move} and similar ones.
 */
public enum ActionSettingType {

    /**
     * This setting configures if we should execute, show, or ask for approval for an action.
     */
    ACTION_MODE("", "", "", "") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting) {
            final EnumSettingDataType enumSettingDataType;
            switch (baseSetting) {
                case Reconfigure:
                    enumSettingDataType = new EnumSettingDataType<>(ActionMode.RECOMMEND,
                            ActionMode.RECOMMEND, null, ActionMode.class);
                    break;
                case Suspend:
                    enumSettingDataType = new EnumSettingDataType<>(ActionMode.MANUAL, null, null,
                        ImmutableMap.<EntityType, ActionMode>builder()
                            .put(EntityType.IO_MODULE, ActionMode.DISABLED)
                            .put(EntityType.APPLICATION_COMPONENT, ActionMode.RECOMMEND)
                            .build(),
                        ActionMode.class);
                    break;
                case Provision:
                    enumSettingDataType = new EnumSettingDataType<>(ActionMode.MANUAL, null, null,
                        ImmutableMap.<EntityType, ActionMode>builder()
                            .put(EntityType.STORAGE_CONTROLLER, ActionMode.DISABLED)
                            .put(EntityType.APPLICATION_COMPONENT, ActionMode.RECOMMEND)
                            .build(),
                        ActionMode.class);
                    break;
                case Move:
                case CloudComputeScale:
                case Resize:
                case ResizeUpDBMem:
                case ResizeDownDBMem:
                case ResizeVcpuUpInBetweenThresholds:
                case ResizeVcpuDownInBetweenThresholds:
                case ResizeVmemUpInBetweenThresholds:
                case ResizeVmemDownInBetweenThresholds:
                case Delete:
                case Activate:
                    enumSettingDataType = new EnumSettingDataType<>(ActionMode.MANUAL, ActionMode.class);
                    break;
                case NonDisruptiveReversibleScaling:
                case NonDisruptiveIrreversibleScaling:
                case DisruptiveReversibleScaling:
                case DisruptiveIrreversibleScaling:
                case DeleteVolume:
                    enumSettingDataType = new EnumSettingDataType<>(ActionMode.MANUAL, null,
                            ActionMode.RECOMMEND, ActionMode.class);
                    break;
                case CloudComputeScaleForPerf:
                case CloudComputeScaleForSavings:
                    enumSettingDataType = new EnumSettingDataType<>(ActionMode.MANUAL, ActionMode.AUTOMATIC, ActionMode.RECOMMEND,  ActionMode.class);
                    break;
                case StorageMove:
                case ResizeUpHeap:
                case ResizeDownHeap:
                case ResizeVcpuAboveMaxThreshold:
                case ResizeVcpuBelowMinThreshold:
                case ResizeVmemAboveMaxThreshold:
                case ResizeVmemBelowMinThreshold:
                default:
                    enumSettingDataType = new EnumSettingDataType<>(ActionMode.RECOMMEND, ActionMode.class);
                    break;
            }

            return Pair.create(createActionModeSettingSpec(baseSetting, enumSettingDataType), null);
        }
    },

    /**
     * Workflow to execute on action generation.
     */
    ON_GEN("onGen", "ActionWorkflow",
            "Workflow to run when we generate a ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting) {
            return Pair.create(createActionWorkflowSettingSpec(baseSetting),
                    ProbeFeature.ACTION_AUDIT);
        }
    },
    /**
     * Workflow to execute after approved, but before execution. Usually used to prepare an action
     * for execution like safely shutting down a machine and saving it's data.
     */
    PRE("pre", "ActionWorkflow",
        "Workflow to run before we execute a ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting) {
            return Pair.create(createActionWorkflowSettingSpec(baseSetting),
                null);
        }
    },
    /**
     * Workflow to execute instead of native execution.
     */
    REPLACE("", "ActionWorkflow",
        "Workflow to run instead of native execution for a ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting) {
            return Pair.create(createActionWorkflowSettingSpec(baseSetting),
                null);
        }
    },
    /**
     * Executed after action finish IN_PROGRESS. Workflow executes after the failed or successful
     * execution of a replace action workflow or a native action.
     */
    POST("post", "ActionWorkflow",
        "Workflow to run after the successful execution for a ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting) {
            return Pair.create(createActionWorkflowSettingSpec(baseSetting),
                null);
        }
    },
    /**
     * Workflow runs after PRE_IN_PROGRESS fails, or POST_IN_PROGRESS succeeds or fails. Cannot run
     * after IN_PROGRESS succeeds or fails because POST must always execute after IN_PROGRESS.
     */
    AFTER_EXEC("afterExec", "ActionWorkflow",
            "Workflow to run when we complete or fail a ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting) {
            return Pair.create(createActionWorkflowSettingSpec(baseSetting),
                    ProbeFeature.ACTION_AUDIT);
        }
    },
    /**
     * Workflow to use for external approval.
     */
    EXTERNAL_APPROVAL("approval", "ActionWorkflow",
            "External Approval Workflow for a ", " action") {

        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting) {
            return Pair.create(createActionWorkflowSettingSpec(baseSetting),
                    ProbeFeature.ACTION_APPROVAL);

        }
    },
    /**
     * Schedule associated with this action mode. This setting is bound to action mode
     * closely and have no sence without action mode setting set.
     */
    SCHEDULE("", "ExecutionSchedule",
            "Execution window for ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting) {
            final SortedSetOfOidSettingDataType dataType = new SortedSetOfOidSettingDataType(
                    Type.ENTITY, Collections.emptySet());
            return Pair.create(createOidSettingSpec(dataType, baseSetting), null);
        }
    };

    private static final StringSettingDataType WORKFLOW_TYPE =
            new StringSettingDataType(null, EntitySettingSpecs.MATCH_ANYTHING_REGEX);

    @Nonnull
    private final String settingNamePrefix;
    @Nonnull
    private final String settingNameSuffix;
    @Nonnull
    private final String displayNamePrefix;
    @Nonnull
    private final String displayNameSuffix;

    ActionSettingType(@Nonnull String settingNamePrefix, @Nonnull String settingNameSuffix,
            @Nonnull String displayNamePrefix, @Nonnull String displayNameSuffix) {
        this.settingNamePrefix = Objects.requireNonNull(settingNamePrefix);
        this.settingNameSuffix = Objects.requireNonNull(settingNameSuffix);
        this.displayNamePrefix = Objects.requireNonNull(displayNamePrefix);
        this.displayNameSuffix = Objects.requireNonNull(displayNameSuffix);
    }

    /**
     * Creates pair of entity spec from the specified base setting with associated probe feature.
     *
     * @param baseSetting base setting to create setting spec from
     * @return pair of subsetting spec with associated probe feature
     */
    @Nonnull
    public abstract Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull ConfigurableActionSettings baseSetting);

    @Nonnull
    private String getSettingName(@Nonnull ConfigurableActionSettings baseSetting) {
        if (settingNamePrefix.isEmpty()) {
            return baseSetting.getSettingName() + settingNameSuffix;
        } else {
            return settingNamePrefix + StringUtils.capitalize(baseSetting.getSettingName())
                    + settingNameSuffix;
        }
    }

    @Nonnull
    private String getDisplayName(@Nonnull ConfigurableActionSettings baseSetting) {
        return displayNamePrefix + baseSetting.getDisplayName() + displayNameSuffix;
    }

    @Nonnull
    private SettingSpec createSettingSpec(
            @Nonnull ConfigurableActionSettings actionModeSettingSpec,
            @Nonnull AbstractSettingDataType<?> dataStructure,
            @Nonnull List<String> categoryPath,
            @Nonnull SettingTiebreaker settingTiebreaker) {
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
                .setName(getSettingName(actionModeSettingSpec))
                .setDisplayName(getDisplayName(actionModeSettingSpec))
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setTiebreaker(settingTiebreaker)
                        .setEntitySettingScope(scopeBuilder)
                        .setAllowGlobalDefault(true));
        if (!categoryPath.isEmpty()) {
            builder.setPath(createSettingCategoryPath(categoryPath));
        }
        dataStructure.build(builder);
        return builder.build();
    }

    @Nonnull
    protected SettingSpec createActionModeSettingSpec(
            @Nonnull ConfigurableActionSettings baseSetting,
            EnumSettingDataType<ActionMode> enumSettingDataType) {
        return createSettingSpec(
            baseSetting,
            enumSettingDataType,
            Collections.emptyList(),
            SettingTiebreaker.SMALLER);
    }

    /**
     * Create the setting specification for ActionWorkflow.
     *
     * @param baseSetting base setting to create an actino related setting for.
     * @return the setting specification describing an on generation or after execution audit
     *         workflow.
     */
    @Nonnull
    protected SettingSpec createActionWorkflowSettingSpec(
            @Nonnull ConfigurableActionSettings baseSetting) {
        return createSettingSpec(
                baseSetting,
                WORKFLOW_TYPE,
                Collections.singletonList(CategoryPathConstants.AUTOMATION),
                SettingTiebreaker.SMALLER);
    }

    @Nonnull
    protected SettingSpec createOidSettingSpec(@Nonnull SortedSetOfOidSettingDataType dataStructure,
            @Nonnull ConfigurableActionSettings baseSetting) {
        return createSettingSpec(baseSetting, dataStructure, Collections.emptyList(),
                SettingTiebreaker.UNION);
    }
}
