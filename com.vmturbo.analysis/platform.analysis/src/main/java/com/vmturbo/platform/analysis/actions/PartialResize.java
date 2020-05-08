package com.vmturbo.platform.analysis.actions;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Trader;

import java.util.HashMap;
import java.util.Map;

public class PartialResize {
    private final Resize resize_;
    private final boolean resizeDueToROI_;
    private final Map<CommoditySold, Trader> rawMaterialsMap_ = new HashMap<>();

    public PartialResize(Resize resize, final boolean resizeDueToROI,
                         Map<CommoditySold, Trader> rawMaterialAndSupplier) {
        this.resize_ = resize;
        this.resizeDueToROI_ = resizeDueToROI;
        rawMaterialsMap_.putAll(rawMaterialAndSupplier);
    }

    public Resize getResize() {
        return resize_;
    }

    public boolean isResizeDueToROI() {
        return resizeDueToROI_;
    }

    public Map<CommoditySold, Trader> getRawMaterials() {
        return rawMaterialsMap_;
    }
}
