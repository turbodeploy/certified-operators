package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Provider related information of a SMAVirtualMachine.
 */
public class SMAVirtualMachineProvider {
    /*
     * The original set of Providers.
     */
    private List<SMATemplate> providers = new ArrayList<>();
    /*
     * List of groupProviders (templates) that this VM could move to.
     * If the VM is in an ASG and not the leader, groupProviders is empty
     */
    private List<SMATemplate> groupProviders = new ArrayList<>();

    /*
     * The least cost provider for this VM in each family.
     * Set in updateNaturalTemplateAndMinCostProviderPerFamily()
     */
    private HashMap<String, SMATemplate> minCostProviderPerFamily = new HashMap<>();

    /*
     * The least cost provider for this VM.
     * Set in updateNaturalTemplateAndMinCostProviderPerFamily() also may be reset if in ASG to
     * the ASG leadres natural template
     */
    private SMATemplate naturalTemplate;

    public void setGroupProviders(List<SMATemplate> groupProviders) {
        this.groupProviders = groupProviders;
    }

    public List<SMATemplate> getGroupProviders() {
        return groupProviders;
    }

    public void setMinCostProviderPerFamily(HashMap<String, SMATemplate> minCostProviderPerFamily) {
        this.minCostProviderPerFamily = minCostProviderPerFamily;
    }

    public HashMap<String, SMATemplate> getMinCostProviderPerFamily() {
        return minCostProviderPerFamily;
    }

    public void setNaturalTemplate(SMATemplate naturalTemplate) {
        this.naturalTemplate = naturalTemplate;
    }

    public SMATemplate getNaturalTemplate() {
        return naturalTemplate;
    }

    public void setProviders(List<SMATemplate> providers) {
        this.providers = providers;
    }

    public List<SMATemplate> getProviders() {
        return providers;
    }
}
