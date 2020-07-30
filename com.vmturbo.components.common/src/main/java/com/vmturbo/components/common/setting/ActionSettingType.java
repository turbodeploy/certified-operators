package com.vmturbo.components.common.setting;

import static com.vmturbo.components.common.setting.SettingDTOUtil.createSettingCategoryPath;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

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
 * which is a setting, holding action mode, i.e. {@link EntitySettingSpecs#Move} and similar ones.
 */
public enum ActionSettingType {

    /**
     * Workflow to execute on action generation.
     */
    ON_GEN("onGen", "ActionWorkflow",
            "Workflow to run when we generate a ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull EntitySettingSpecs baseSetting) {
            return Pair.create(createActionWorkflowSettingSpec(baseSetting),
                    ProbeFeature.ACTION_AUDIT);
        }
    },
    /**
     * Workflow to execute after action execution.
     */
    AFTER_EXEC("afterExec", "ActionWorkflow",
            "Workflow to run when we complete or fail a ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull EntitySettingSpecs baseSetting) {
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
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull EntitySettingSpecs baseSetting) {
            return Pair.create(createActionWorkflowSettingSpec(baseSetting),
                    ProbeFeature.ACTION_APPROVAL);
        }
    },
    /**
     * Schedule associated with this action mode. This setting is bound to action mode
     * closely and have no sence without action mode setting set.
     */
    SCHEDULE(null, "ExecutionSchedule",
            "Execution window for ", " action") {
        @Nonnull
        @Override
        public Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull EntitySettingSpecs baseSetting) {
            final SortedSetOfOidSettingDataType dataType = new SortedSetOfOidSettingDataType(
                    Type.ENTITY, Collections.emptySet());
            return Pair.create(createOidSettingSpec(dataType, baseSetting), null);
        }
    };

    private static final StringSettingDataType WORKFLOW_TYPE =
            new StringSettingDataType(null, EntitySettingSpecs.MATCH_ANYTHING_REGEX);

    private final String settingNamePrefix;
    private final String settingNameSuffix;
    private final String displayNamePrefix;
    private final String displayNameSuffix;

    ActionSettingType(@Nullable String settingNamePrefix, @Nonnull String settingNameSuffix,
            @Nonnull String displayNamePrefix, @Nonnull String displayNameSuffix) {
        this.settingNamePrefix = settingNamePrefix;
        this.settingNameSuffix = settingNameSuffix;
        this.displayNamePrefix = displayNamePrefix;
        this.displayNameSuffix = displayNameSuffix;
    }

    /**
     * Creates pair of entity spec from the specified base setting with associated probe feature.
     *
     * @param baseSetting base setting to create setting spec from
     * @return pair of subsetting spec with associated probe feature
     */
    @Nonnull
    public abstract Pair<SettingSpec, ProbeFeature> createEntitySpec(@Nonnull EntitySettingSpecs baseSetting);

    @Nonnull
    private String getSettingName(@Nonnull EntitySettingSpecs baseSetting) {
        if (settingNamePrefix == null) {
            return baseSetting.getSettingName() + settingNameSuffix;
        } else {
            return settingNamePrefix + StringUtils.capitalize(baseSetting.getSettingName())
                    + settingNameSuffix;
        }
    }

    @Nonnull
    private String getDisplayName(@Nonnull EntitySettingSpecs baseSetting) {
        return displayNamePrefix + baseSetting.getDisplayName() + displayNameSuffix;
    }

    @Nonnull
    private SettingSpec createSettingSpec(
            @Nonnull EntitySettingSpecs actionModeSettingSpec,
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
                        .setAllowGlobalDefault(actionModeSettingSpec.isAllowGlobalDefault()));
        if (!categoryPath.isEmpty()) {
            builder.setPath(createSettingCategoryPath(categoryPath));
        }
        dataStructure.build(builder);
        return builder.build();
    }


    /**
     * Create the setting specification in the same way that {@link EntitySettingSpecs} creates
     * ActionWorkflow settings like {@link EntitySettingSpecs#PreActivateActionWorkflow}.
     *
     * @param baseSetting base setting to create an actino related setting for.
     * @return the setting specification describing an on generation or after execution audit
     *         workflow.
     */
    @Nonnull
    protected SettingSpec createActionWorkflowSettingSpec(
            @Nonnull EntitySettingSpecs baseSetting) {
        return createSettingSpec(
                baseSetting,
                WORKFLOW_TYPE,
                Collections.singletonList(CategoryPathConstants.AUTOMATION),
                SettingTiebreaker.SMALLER);
    }

    @Nonnull
    protected SettingSpec createOidSettingSpec(@Nonnull SortedSetOfOidSettingDataType dataStructure,
            @Nonnull EntitySettingSpecs baseSetting) {
        return createSettingSpec(baseSetting, dataStructure, Collections.emptyList(),
                SettingTiebreaker.UNION);
    }
}
