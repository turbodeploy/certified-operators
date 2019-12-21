package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * The Stable Marriage algorithm input context.
 * The context contains all the VMs, RIs and templates that are scoped to this input context.
 * Given the list of VMs, RIs and templates, all other information is superfluous, and only
 * used for debugging.
 */
public class SMAInputContext {

    /*
     * The context
     */
    private final SMAContext context;
    /*
     * List of virtual machines
     */
    private final List<SMAVirtualMachine> virtualMachines;

    /*
     * List of reserved instances.  There may be no RIs.
     */
    private final List<SMAReservedInstance> reservedInstances;
    /*
     * List of templates; that is, providers
     */
    private final List<SMATemplate> templates;

    /**
     * The constructor for SMAInputContext.
     *
     * @param context the current context
     * @param virtualMachines the virtual machines in this context
     * @param reservedInstances the reserved instance in this context
     * @param templates the templates in this context
     */
    public SMAInputContext(final SMAContext context,
                           @Nonnull final List<SMAVirtualMachine> virtualMachines,
                           final List<SMAReservedInstance> reservedInstances,
                           @Nonnull final List<SMATemplate> templates) {
        this.context = context;

        this.virtualMachines = Objects.requireNonNull(virtualMachines, "virutalMachines is null!");
        this.reservedInstances = reservedInstances;
        this.templates = Objects.requireNonNull(templates, "templates is null!");
    }

    @Nonnull
    public SMAContext getContext() {
        return context;
    }

    @Nonnull
    public List<SMAVirtualMachine> getVirtualMachines() {
        return virtualMachines;
    }

    @Nonnull
    public List<SMAReservedInstance> getReservedInstances() {
        return reservedInstances;
    }

    @Nonnull
    public List<SMATemplate> getTemplates() {
        return templates;
    }

    @Override
    public String toString() {
        return "SMAInputContext{" +
                "context=" + context +
                ", virtualMachines=" + virtualMachines.size() +
                ", reservedInstances=" + reservedInstances.size() +
                ", templates=" + templates.size() +
                '}';
    }
}

