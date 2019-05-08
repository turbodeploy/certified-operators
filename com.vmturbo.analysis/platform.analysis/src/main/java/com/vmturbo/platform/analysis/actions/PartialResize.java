package com.vmturbo.platform.analysis.actions;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.sdk.common.util.Pair;

public class PartialResize {
    private final Resize resize_;
    private final boolean resizeDueToROI_;
    private final CommoditySold rawMaterial_;
    private final Trader supplier_;

    public PartialResize(Resize resize, final boolean resizeDueToROI,
                         Pair<CommoditySold, Trader> rawMaterialAndSupplier) {
        this.resize_ = resize;
        this.resizeDueToROI_ = resizeDueToROI;
        this.rawMaterial_ = rawMaterialAndSupplier.getFirst();
        this.supplier_ = rawMaterialAndSupplier.getSecond();
    }

    public Resize getResize() {
        return resize_;
    }

    public boolean isResizeDueToROI() {
        return resizeDueToROI_;
    }

    public CommoditySold getRawMaterial() {
        return rawMaterial_;
    }

    public Trader getSupplier() {
        return supplier_;
    }
}
