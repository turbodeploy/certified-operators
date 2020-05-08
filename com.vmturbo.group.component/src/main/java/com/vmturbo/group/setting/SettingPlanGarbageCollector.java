package com.vmturbo.group.setting;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector;

/**
 * Responsible for deleting settings data related to plans for the group component.
 */
public class SettingPlanGarbageCollector implements PlanGarbageCollector {
    private SettingStore settingStore;

    /**
     * Creates a new instance.
     *
     * @param settingStore setting store
     */
    public SettingPlanGarbageCollector(SettingStore settingStore) {
        this.settingStore = settingStore;
    }

    @Nonnull
    @Override
    public List<ListExistingPlanIds> listPlansWithData() {
        return Collections.singletonList(settingStore::getContextsWithSettings);
    }

    @Override
    public void deletePlanData(final long planId) {
        settingStore.deletePlanSettings(planId);
    }
}
