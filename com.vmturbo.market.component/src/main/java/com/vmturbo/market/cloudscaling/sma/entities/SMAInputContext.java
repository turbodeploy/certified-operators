package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
     * The config
     */
    private SMAConfig smaConfig = new SMAConfig();
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
     * The constructor for SMAInputContext with config.
     *
     * @param context the current context
     * @param virtualMachines the virtual machines in this context
     * @param reservedInstances the reserved instance in this context
     * @param templates the templates in this context
     * @param smaConfig the config
     */
    public SMAInputContext(@Nonnull final SMAContext context,
                           @Nonnull final List<SMAVirtualMachine> virtualMachines,
                           final List<SMAReservedInstance> reservedInstances,
                           @Nonnull final List<SMATemplate> templates,
                           @Nonnull final SMAConfig smaConfig) {
        this.context = Objects.requireNonNull(context, "context is null!");
        this.virtualMachines = Objects.requireNonNull(virtualMachines, "virutalMachines is null!");
        this.reservedInstances = reservedInstances;
        this.templates = Objects.requireNonNull(templates, "templates is null!");
        this.smaConfig = smaConfig;
    }

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
        this.smaConfig = inputContext.getSmaConfig();
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
                    oldVM.getCurrentRI(),
                    oldVM.getOsType());
            newVirtualMachines.add(smaVirtualMachine);
        }
        this.virtualMachines = newVirtualMachines;
        List<SMAReservedInstance> newReservedInstances = new ArrayList<>();
        List<SMAReservedInstance> oldReservedInstances = inputContext.getReservedInstances();
        if (oldReservedInstances != null) {
            for (int i = 0; i < oldReservedInstances.size(); i++) {
                SMAReservedInstance oldRI = oldReservedInstances.get(i);
                SMAReservedInstance newRI = SMAReservedInstance.copyFrom(oldRI);
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
        this.smaConfig = inputContext.getSmaConfig();
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
                    smaMatch.getDiscountedCoupons(),
                    oldVM.getZoneId(),
                    smaMatch.getReservedInstance() == null ?
                            SMAUtils.BOGUS_RI :
                            smaMatch.getReservedInstance(),
                    oldVM.getOsType());
            newVirtualMachines.add(smaVirtualMachine);
        }
        this.virtualMachines = newVirtualMachines;
        List<SMAReservedInstance> newReservedInstances = new ArrayList<>();
        for (SMAReservedInstance oldRI : inputContext.getReservedInstances()) {
            SMAReservedInstance newRI = SMAReservedInstance.copyFrom(oldRI);
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

    /**
     * getter for smaConfig.
     * @return the smaConfig.
     */
    @Nonnull
    public SMAConfig getSmaConfig() {
        return smaConfig;
    }

    /**
     * setter for smaConfig.
     * @param smaConfig the new smaConfig
     */
    public void setSmaConfig(final SMAConfig smaConfig) {
        this.smaConfig = smaConfig;
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

    // Compression for diags related code



    /**
     * decompress inputContext.
     */
    public void decompress() {
        Map<Long, SMATemplate> oidToTemplateMap = new HashMap();

        Map<Long, SMAReservedInstance> oidToRIMap = new HashMap();
        getTemplates().stream().forEach(template -> {
            oidToTemplateMap.put(template.getOid(), template);
        });
        getReservedInstances().stream().forEach(ri -> {
            oidToRIMap.put(ri.getOid(), ri);
        });
        getVirtualMachines().stream().forEach(vm -> {
            vm.setCurrentTemplate(oidToTemplateMap.get(vm.getCurrentTemplateOid()));
            vm.setCurrentRI(oidToRIMap.get(vm.getCurrentRIOID()));
            List<SMATemplate> providerList = (vm.getProvidersOid()
                    .stream().map(oid -> oidToTemplateMap.get(oid)).collect(Collectors.toList()));
            vm.setProviders(providerList);
            vm.getProvidersOid().clear();
        });
        getReservedInstances().stream().forEach(ri -> {
            ri.setTemplate(oidToTemplateMap.get(ri.getTemplateOid()));
            ri.setNormalizedTemplate(oidToTemplateMap.get(ri.getTemplateOid()));
        });

    }

    /**
     * compress inputContext.
     */
    public void compress() {
        getVirtualMachines().stream().forEach(vm -> {
            vm.getProvidersOid().clear();
            if (vm.getProviders() != null) {
                vm.getProvidersOid().addAll(vm.getProviders()
                        .stream().map(provider -> provider.getOid())
                        .collect(Collectors.toList()));
                vm.getProviders().clear();
            }
            vm.setGroupProviders(new ArrayList<>());
            vm.setCurrentTemplateOid(vm.getCurrentTemplate().getOid());
            vm.setCurrentTemplate(null);
            vm.setNaturalTemplate(null);
            if (vm.getCurrentRI() != null) {
                vm.setCurrentRIOID(vm.getCurrentRI().getOid());
                vm.setCurrentRI(null);
            }
        });

        getReservedInstances().stream().forEach(ri -> {
            ri.setTemplateOid(ri.getTemplate().getOid());
            ri.setTemplate(null);
            ri.setNormalizedTemplate(null);
        });
    }
}

