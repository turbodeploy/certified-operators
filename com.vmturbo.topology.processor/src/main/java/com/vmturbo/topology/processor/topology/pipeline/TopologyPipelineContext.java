package com.vmturbo.topology.processor.topology.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo.Builder;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.pipeline.PipelineContext;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.settings.SettingPolicyEditor;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal.StitchingJournalContainer;

/**
 * The {@link TopologyPipelineContext} is information that's shared by all stages
 * in a pipeline.
 * <p>
 * This is the place to put generic info that applies to many stages, as well as utility objects
 * that have some state (e.g. caches like {@link GroupResolver}) which are shared across
 * the stages.
 * <p>
 * The context is immutable, but objects inside it may be mutable.
 */
public class TopologyPipelineContext implements PipelineContext {
    private final GroupResolver groupResolver;

    private final TopologyInfo.Builder topologyInfoBuilder;

    private final StitchingJournalContainer stitchingJournalContainer;
    private final ConsistentScalingManager consistentScalingManager;

    /**
     * For plans like MPC, this is the set of entities that are selected for migration to a
     * target CSP. This is used by various stages of the plan pipeline, thus stored here.
     */
    private final Set<Long> sourceEntities;

    /**
     * Filtered applicable destination entity oids - e.g target region ids for MCP case.
     */
    private final Set<Long> destinationEntities;

    /**
     * Hooks that stages prior to SettingsResolutionStage can register to modify
     * the settings policies applied.
     */
    private final List<SettingPolicyEditor> settingPolicyEditors;

    /**
     * Grouping info (if set) that is used to create placement policies later in PolicyStage.
     * This is mainly used for cloud migration case, where first element of pair is the grouping
     * for source entities being migrated, and second element is the target region group that the
     * entities are being migrated to.
     * These groupings are later used by PolicyManager for creating placement policies.
     */
    private final Set<Pair<Grouping, Grouping>> policyGroups;

    /**
     * If set, contains the names of post-stitching operations that should be skipped.
     */
    private Set<String> postStitchingOperationsToSkip;

    public TopologyPipelineContext(@Nonnull final GroupResolver groupResolver,
                                   @Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final ConsistentScalingManager consistentScalingManager) {
        this.groupResolver = Objects.requireNonNull(groupResolver);
        this.topologyInfoBuilder = Objects.requireNonNull(TopologyInfo.newBuilder(topologyInfo));
        this.stitchingJournalContainer = new StitchingJournalContainer();
        this.consistentScalingManager = consistentScalingManager;
        sourceEntities = new HashSet<>();
        destinationEntities = new HashSet<>();
        policyGroups = new HashSet<>();
        settingPolicyEditors = new ArrayList<>();
        postStitchingOperationsToSkip = new HashSet<>();
    }

    @Nonnull
    public GroupResolver getGroupResolver() {
        return groupResolver;
    }

    @Nonnull
    public TopologyInfo getTopologyInfo() {
        return topologyInfoBuilder.buildPartial();
    }

    public void editTopologyInfo(Consumer<Builder> editFunction) {
        editFunction.accept(topologyInfoBuilder);
    }

    /**
     * Sets the set of source entity oids in this context.
     *
     * @param sourceOids Set of source entity oids.
     */
    public void setSourceEntities(@Nonnull final Collection<Long> sourceOids) {
        sourceEntities.clear();
        sourceEntities.addAll(sourceOids);
    }

    /**
     * Sets the set of destination entity oids in this context.
     *
     * @param destinationOids Set of destination entity oids.
     */
    public void setDestinationEntities(@Nonnull final Collection<Long> destinationOids) {
        destinationEntities.clear();
        destinationEntities.addAll(destinationOids);
    }

    /**
     * Gets the set of source entity oids.
     *
     * @return Set of source entity oids.
     */
    @Nonnull
    public Set<Long> getSourceEntities() {
        return Collections.unmodifiableSet(sourceEntities);
    }

    /**
     * Gets the set of destination entity oids.
     *
     * @return Set of destination entity oids.
     */
    @Nonnull
    public Set<Long> getDestinationEntities() {
        return Collections.unmodifiableSet(destinationEntities);
    }

    /**
     * Gets policy groups which are used to create placement policies.
     *
     * @return Policy group source and destination pairs.
     */
    @Nonnull
    public Set<Pair<Grouping, Grouping>> getPolicyGroups() {
        return Collections.unmodifiableSet(policyGroups);
    }

    /**
     * Clears policy groups currently set if any.
     */
    public void clearPolicyGroups() {
        policyGroups.clear();
    }

    /**
     * Adds a policy group to the set.
     *
     * @param group Policy group to add.
     */
    public void addPolicyGroup(@Nonnull final Pair<Grouping, Grouping> group) {
        policyGroups.add(group);
    }

    /**
     * Any optional operations that need to be skipped for post-stitching stage. E.g, for cloud
     * migration, certain operation that makes HyperV VM's unmovable need to be skipped, so that
     * such VMs can be migrated to cloud.
     *
     * @param operationNames Names of operations to skip.
     */
    public void setPostStitchingOperationsToSkip(@Nonnull final Set<String> operationNames) {
        postStitchingOperationsToSkip.clear();
        postStitchingOperationsToSkip.addAll(operationNames);
    }

    /**
     * Gets optional post-stitching operations to skip.
     *
     * @return Post-stitching operations to skip.
     */
    @Nonnull
    public Set<String> getPostStitchingOperationsToSkip() {
        return Collections.unmodifiableSet(postStitchingOperationsToSkip);
    }

    public String getTopologyTypeName() {
        return topologyInfoBuilder.getTopologyType().name();
    }

    @Nonnull
    public StitchingJournalContainer getStitchingJournalContainer() {
        return stitchingJournalContainer;
    }

    @Nonnull
    public ConsistentScalingManager getConsistentScalingManager() {
        return consistentScalingManager;
    }

    @NotNull
    @Override
    public String getPipelineName() {
        return FormattedString.format("Topology Pipeline (context : {}, id : {})",
            topologyInfoBuilder.getTopologyContextId(), topologyInfoBuilder.getTopologyId());
    }

    /**
     * Get the list of setting policies editor hooks to be applied by the SettingsResolutionStage.
     *
     * @return a list of editor methods to be called.
     */
    @Nonnull
    public List<SettingPolicyEditor> getSettingPolicyEditors() {
        return settingPolicyEditors;
    }

    /**
     * Add a setting policies editor hook to be applied by the SettingsResolutionStage.
     *
     * @param settingPolicyEditor a hook to be applied for editing setting policies.
     */
    @Nonnull
    public void addSettingPolicyEditor(@Nonnull SettingPolicyEditor settingPolicyEditor) {
        settingPolicyEditors.add(settingPolicyEditor);
    }
}
