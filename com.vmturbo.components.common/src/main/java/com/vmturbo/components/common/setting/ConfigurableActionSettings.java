package com.vmturbo.components.common.setting;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * All settings that have ActionMode, Schedule, Action Script values.
 * Separate from {@link EntitySettingSpecs} to make it
 * easier to add new settings with action modes in the future like the recently added
 * CloudComputeScale setting. Secondly, {@link EntitySettingSpecs} has become far to large to
 * add new settings to.
 */
public enum ConfigurableActionSettings {

    /**
     * Move action automation mode.
     */
    Move("move", "Move",
        EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD,
            EntityType.CONTAINER, EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
            EntityType.BUSINESS_USER)),

    /**
     * Cloud compute scale action automation mode.
     */
    CloudComputeScale("cloudComputeScale", "Cloud Compute Scale",
        EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.DATABASE, EntityType.DATABASE_SERVER)),

    /**
     * Storage Move action automation mode.
     */
    StorageMove("storageMove", "Storage Move",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Resize action automation mode.
     *
     *<p>For VM, this setting is only being used for commodities other than cpu, vcpu, mem and vmem.
     * The reason is that those commodities are handled by their specific settings,
     * such as ResizeVcpuUpInBetweenThresholds.</p>
     */
    Resize("resize", "Resize",
        EnumSet.of(EntityType.STORAGE, EntityType.CONTAINER,
            EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
            EntityType.DATABASE_SERVER, EntityType.WORKLOAD_CONTROLLER)),

    /**
     * Resize Up Heap automation mode.
     */
    ResizeUpHeap("resizeUpHeap", "Resize Up Heap",
        EnumSet.of(EntityType.APPLICATION_COMPONENT)),

    /**
     * Resize Down Heap automation mode.
     */
    ResizeDownHeap("resizeDownHeap", "Resize Down Heap",
        EnumSet.of(EntityType.APPLICATION_COMPONENT)),

    /**
     * Resize Up DBMem automation mode.
     */
    ResizeUpDBMem("resizeUpDBMem", "Resize Up DBMem",
        EnumSet.of(EntityType.DATABASE_SERVER)),

    /**
     * Resize Down DBMem automation mode.
     */
    ResizeDownDBMem("resizeDownDBMem", "Resize Down DBMem",
        EnumSet.of(EntityType.DATABASE_SERVER)),

    /**
     * Resize action automation mode for vcpu resize ups where the target capacity is between
     * {@link EntitySettingSpecs#ResizeVcpuMinThreshold} and {@link EntitySettingSpecs#ResizeVcpuMaxThreshold}.
     */
    ResizeVcpuUpInBetweenThresholds("resizeVcpuUpInBetweenThresholds", "VCPU Resize Up",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Resize action automation mode for vcpu resize downs where the target capacity is between
     * {@link EntitySettingSpecs#ResizeVcpuMinThreshold} and {@link EntitySettingSpecs#ResizeVcpuMaxThreshold}.
     */
    ResizeVcpuDownInBetweenThresholds("resizeVcpuDownInBetweenThresholds", "VCPU Resize Down",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Resize action automation mode for vcpu resizes where the target capacity is above the max threshold value {@link EntitySettingSpecs#ResizeVcpuMaxThreshold}.
     */
    ResizeVcpuAboveMaxThreshold("resizeVcpuAboveMaxThreshold", "VCPU Resize Above Max",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Resize action automation mode for vcpu resizes where the target capacity is below the min value {@link EntitySettingSpecs#ResizeVcpuMinThreshold}.
     */
    ResizeVcpuBelowMinThreshold("resizeVcpuBelowMinThreshold", "VCPU Resize Below Min",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Resize action automation mode for vmem resize ups where the target capacity is between
     * {@link EntitySettingSpecs#ResizeVmemMinThreshold} and {@link EntitySettingSpecs#ResizeVmemMaxThreshold}.
     */
    ResizeVmemUpInBetweenThresholds("resizeVmemUpInBetweenThresholds", "VMem Resize Up",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Resize action automation mode for vmem resize downs where the target capacity is between
     * {@link EntitySettingSpecs#ResizeVmemMinThreshold} and {@link EntitySettingSpecs#ResizeVmemMaxThreshold}.
     */
    ResizeVmemDownInBetweenThresholds("resizeVmemDownInBetweenThresholds", "VMem Resize Down",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Resize action automation mode for vmem resizes where the target capacity is above the max threshold value {@link EntitySettingSpecs#ResizeVmemMaxThreshold}.
     */
    ResizeVmemAboveMaxThreshold("resizeVmemAboveMaxThreshold", "VMem Resize Above Max",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Resize action automation mode for vmem resizes where the target capacity is below the min value {@link EntitySettingSpecs#ResizeVmemMinThreshold}.
     */
    ResizeVmemBelowMinThreshold("resizeVmemBelowMinThreshold", "VMem Resize Below Min",
        EnumSet.of(EntityType.VIRTUAL_MACHINE)),

    /**
     * Suspend action automation mode.
     */
    Suspend("suspend", "Suspend",
        EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
            EntityType.CONTAINER_POD, EntityType.CONTAINER,
            EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
            EntityType.APPLICATION_COMPONENT, EntityType.IO_MODULE)),

    /**
     * Delete action automation mode.
     */
    Delete("delete", "Delete",
        EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_VOLUME)),

    /**
     * Delete Volume action automation mode.
     */
    DeleteVolume("deleteVolume", "DeleteVolume",
        EnumSet.of(EntityType.VIRTUAL_VOLUME)),

    /**
     * Provision action automation mode.
     */
    Provision("provision", "Provision",
        EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.DISK_ARRAY,
            EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD, EntityType.CONTAINER,
            EntityType.LOGICAL_POOL, EntityType.STORAGE_CONTROLLER,
            EntityType.APPLICATION_COMPONENT)),

    /**
     * Reconfigure action automation mode (not executable).
     */
    Reconfigure("reconfigure", "Reconfigure",
        EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD)),

    /**
     * Activate action automation mode.
     */
    Activate("activate", "Start",
        EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
            EntityType.CONTAINER_POD, EntityType.CONTAINER,
            EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL)),

    /**
     * Automation mode for non-disruptive reversible actions.
     */
    NonDisruptiveReversibleScaling("nonDisruptiveReversibleScaling",
        "Non-disruptive Reversible Scaling", EnumSet.of(EntityType.VIRTUAL_VOLUME)),

    /**
     * Automation mode for non-disruptive irreversible actions.
     */
    NonDisruptiveIrreversibleScaling("nonDisruptiveIrreversibleScaling",
        "Non-disruptive Irreversible Scaling", EnumSet.of(EntityType.VIRTUAL_VOLUME)),

    /**
     * Automation mode for disruptive reversible actions.
     */
    DisruptiveReversibleScaling("disruptiveReversibleScaling",
        "Disruptive Reversible Scaling", EnumSet.of(EntityType.VIRTUAL_VOLUME)),

    /**
     * Automation mode for disruptive irreversible actions.
     */
    DisruptiveIrreversibleScaling("disruptiveIrreversibleScaling",
        "Disruptive Irreversible Scaling", EnumSet.of(EntityType.VIRTUAL_VOLUME));

    private static final Map<String, ConfigurableActionSettings> SETTINGS_MAP;

    static {
        SETTINGS_MAP = Arrays.stream(ConfigurableActionSettings.values())
            .collect(Collectors.toMap(
                ConfigurableActionSettings::getSettingName,
                Function.identity()));
    }

    private final String name;
    private final String displayName;
    private final Set<EntityType> entityTypeScope;

    /**
     * Create an ActionModeSettingSpec, representing a setting attached to an entity or group.
     *
     * @param name the name (also called 'uuid') of this setting
     * @param displayName A human-readable display name for the setting.
     * @param entityTypeScope enumeration of entity types that this setting may apply to
     */
    ConfigurableActionSettings(@Nonnull String name,
                               @Nonnull String displayName,
                               @Nonnull Set<EntityType> entityTypeScope) {
        this.name = Objects.requireNonNull(name);
        this.displayName = Objects.requireNonNull(displayName);
        this.entityTypeScope = Objects.requireNonNull(entityTypeScope);
    }

    /**
     * Returns setting name, identified by this enumeration value.
     *
     * @return setting name
     */
    @Nonnull
    public String getSettingName() {
        return name;
    }

    /**
     * Returns setting name, identified by this enumeration value.
     *
     * @return setting name
     */
    @Nonnull
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Returns scope of entity type applicable for this setting.
     * @return set of entities type in scope
     */
    public Set<EntityType> getEntityTypeScope() {
        return entityTypeScope;
    }

    /**
     * Get the ConfigurableActionSettings from the setting name.
     *
     * @param settingName the name of the setting.
     * @return null if not found. Otherwise, returns the matching ConfigurableActionSettings.
     */
    @Nullable
    public static ConfigurableActionSettings fromSettingName(@Nonnull String settingName) {
        return SETTINGS_MAP.get(settingName);
    }
}
