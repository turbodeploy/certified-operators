package com.vmturbo.topology.processor.topology.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.reflect.TypeToken;

import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicy;
import com.vmturbo.topology.processor.group.settings.SettingPolicyEditor;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal.StitchingJournalContainer;

/**
 * Definitions for {@link PipelineContextMemberDefinition}s for the {@link TopologyPipelineContext}.
 */
public class TopologyPipelineContextMembers {

    /**
     * Hide constructor for utility class.
     */
    private TopologyPipelineContextMembers() {

    }

    /**
     * If set, contains the names of post-stitching operations that should be skipped.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<Set<String>> POST_STITCHING_OPERATIONS_TO_SKIP =
        PipelineContextMemberDefinition.memberWithDefault(
            (Class<Set<String>>)(new TypeToken<Set<String>>() { }).getRawType(),
            () -> "postStitchingOperationsToSkip",
            HashSet::new,
            set -> "size=" + set.size());

    /**
     * The group resolver for resolving groups.
     */
    public static final PipelineContextMemberDefinition<GroupResolver> GROUP_RESOLVER =
        PipelineContextMemberDefinition.member(GroupResolver.class, "groupResolver",
            resolver -> "cacheSize=" + resolver.getCacheSize());

    /**
     * Container for storing the stitching journal.
     */
    public static final PipelineContextMemberDefinition<StitchingJournalContainer> STITCHING_JOURNAL_CONTAINER =
        PipelineContextMemberDefinition.member(StitchingJournalContainer.class, "stitchingJournalContainer",
            container -> null);

    /**
     * For plans like MPC, this is the set of entities that are selected for migration to a
     * target CSP. This is used by various stages of the plan pipeline, thus stored here.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<Set<Long>> PLAN_SOURCE_ENTITIES =
        PipelineContextMemberDefinition.memberWithDefault(
            (Class<Set<Long>>)(new TypeToken<Set<Long>>() { }).getRawType(),
            () -> "planSourceEntities",
            HashSet::new,
            set -> "size=" + set.size());

    /**
     * Filtered applicable destination entity OIDs - e.g target region ids for MCP case.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<Set<Long>> PLAN_DESTINATION_ENTITIES =
        PipelineContextMemberDefinition.memberWithDefault(
            (Class<Set<Long>>)(new TypeToken<Set<Long>>(){}).getRawType(),
            () -> "planDestinationEntities",
            HashSet::new,
            set -> "size=" + set.size());

    /**
     * Hooks that stages prior to SettingsResolutionStage can register to modify
     * the settings policies applied.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<List<SettingPolicyEditor>> SETTING_POLICY_EDITORS =
        PipelineContextMemberDefinition.memberWithDefault(
            (Class<List<SettingPolicyEditor>>)(new TypeToken<List<SettingPolicyEditor>>(){}).getRawType(),
            () -> "settingPolicyEditors",
            ArrayList::new,
            list -> "size=" + list.size());

    /**
     * Placement policies to be applied later in PolicyStage.  This is mainly used for migration
     * cases, where a group of source entities being migrated is restricted being placed within a
     * group of destination entities.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<List<PlacementPolicy>> PLACEMENT_POLICIES =
        PipelineContextMemberDefinition.memberWithDefault(
            (Class<List<PlacementPolicy>>)(new TypeToken<List<PlacementPolicy>>(){ }).getRawType(),
            () -> "placementPolicies",
            ArrayList::new,
            list -> "size=" + list.size());

    /**
     * This context is storing Groups with their members, this is useful in case of Dynamic Group
     * that use some criteria not available in later stages.
     */
    @SuppressWarnings("unchecked")
    public static final PipelineContextMemberDefinition<Map<Long, ResolvedGroup>> RESOLVED_GROUPS =
            PipelineContextMemberDefinition.memberWithDefault(
                    (Class<Map<Long, ResolvedGroup>>)(new TypeToken<Map<Long, ResolvedGroup>>(){ }).getRawType(),
                    () -> "resolvedGroups",
                    HashMap::new,
                    map -> "size=" + map.size());
}
