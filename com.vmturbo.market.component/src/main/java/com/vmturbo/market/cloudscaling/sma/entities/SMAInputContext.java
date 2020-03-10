package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;

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
    public SMAInputContext(@Nonnull final SMAContext context,
                           @Nonnull final List<SMAVirtualMachine> virtualMachines,
                           final List<SMAReservedInstance> reservedInstances,
                           @Nonnull final List<SMATemplate> templates) {
        this.context = Objects.requireNonNull(context, "context is null!");
        this.virtualMachines = Objects.requireNonNull(virtualMachines, "virutalMachines is null!");
        this.reservedInstances = reservedInstances;
        this.templates = Objects.requireNonNull(templates, "templates is null!");
    }

    /**
     * Create a new input context based on the current input Context.
     *
     * @param inputContext current input context
     */
    public SMAInputContext(@Nonnull final SMAInputContext inputContext) {
        this.context = inputContext.getContext();
        this.templates = inputContext.getTemplates();
        List<SMAVirtualMachine> newVirtualMachines = new ArrayList<>();
        for (SMAVirtualMachine oldVM : inputContext.getVirtualMachines()) {
            SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(oldVM.getOid(),
                    oldVM.getName(),
                    oldVM.getGroupName(),
                    oldVM.getBusinessAccountId(),
                    oldVM.getCurrentTemplate(),
                    oldVM.getProviders(),
                    oldVM.getCurrentRICoverage(),
                    oldVM.getZoneId(),
                    oldVM.getCurrentRIKey(),
                    oldVM.getOsType());
            smaVirtualMachine.updateNaturalTemplateAndMinCostProviderPerFamily();
            newVirtualMachines.add(smaVirtualMachine);
        }
        this.virtualMachines = newVirtualMachines;
        List<SMAReservedInstance> newReservedInstances = new ArrayList<>();
        List<SMAReservedInstance> oldReservedInstances = inputContext.getReservedInstances();
        if (oldReservedInstances != null) {
            for (int i = 0; i < oldReservedInstances.size(); i++) {
                SMAReservedInstance oldRI = oldReservedInstances.get(i);
                SMAReservedInstance newRI = new SMAReservedInstance(oldRI.getOid(),
                    oldRI.getRiKeyOid(),
                    oldRI.getName(),
                    oldRI.getBusinessAccountId(),
                    oldRI.getTemplate(),
                    oldRI.getZoneId(),
                    oldRI.getCount(),
                    oldRI.isIsf(),
                    oldRI.isShared(),
                    oldRI.isPlatformFlexible());
                newReservedInstances.add(newRI);
            }
        }
        this.reservedInstances = newReservedInstances;
    }

    /**
     * Create a new input context from the current input context. The current template and
     * the RI utilization of the VM is updated based on the outputContext.
     *
     * @param inputContext  the current input context.
     * @param outputContext the current output context.
     */
    public SMAInputContext(@Nonnull final SMAInputContext inputContext,
                           @Nonnull final SMAOutputContext outputContext) {
        this.context = inputContext.getContext();
        this.templates = inputContext.getTemplates();
        List<SMAVirtualMachine> newVirtualMachines = new ArrayList<>();
        for (SMAMatch smaMatch : outputContext.getMatches()) {
            SMAVirtualMachine oldVM = smaMatch.getVirtualMachine();
            SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(oldVM.getOid(),
                    oldVM.getName(),
                    oldVM.getGroupName(),
                    oldVM.getBusinessAccountId(),
                    smaMatch.getTemplate(),
                    oldVM.getProviders(),
                    (float)smaMatch.getDiscountedCoupons(),
                    oldVM.getZoneId(),
                    smaMatch.getReservedInstance() == null ?
                            SMAUtils.NO_CURRENT_RI :
                            smaMatch.getReservedInstance().getRiKeyOid(),
                    oldVM.getOsType());
            smaVirtualMachine.updateNaturalTemplateAndMinCostProviderPerFamily();
            newVirtualMachines.add(smaVirtualMachine);
        }
        this.virtualMachines = newVirtualMachines;
        List<SMAReservedInstance> newReservedInstances = new ArrayList<>();
        for (SMAReservedInstance oldRI : inputContext.getReservedInstances()) {
            SMAReservedInstance newRI = new SMAReservedInstance(
                    oldRI.getOid(),
                    oldRI.getRiKeyOid(),
                    oldRI.getName(),
                    oldRI.getBusinessAccountId(),
                    oldRI.getTemplate(),
                    oldRI.getZoneId(),
                    oldRI.getCount(),
                    oldRI.isIsf(),
                    oldRI.isShared(),
                    oldRI.isPlatformFlexible());
            newReservedInstances.add(newRI);
        }
        this.reservedInstances = newReservedInstances;
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
                ", reservedInstances=" + reservedInstances == null ? "" + 0 : reservedInstances.size() +
                ", templates=" + templates.size() +
                '}';
    }
}

