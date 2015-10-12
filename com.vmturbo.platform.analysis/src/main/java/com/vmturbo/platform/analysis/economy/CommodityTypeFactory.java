package com.vmturbo.platform.analysis.economy;

import static com.vmturbo.platform.analysis.economy.NumericCommodityType.composeNumericalRepresentation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class CommodityTypeFactory {
    // Fields
    private Map<String,Short> numericalKinds = new TreeMap<String,Short>(); // the association between human
        // readable strings representing the kind of commodity and numerical kinds used internally
        // for performance.
    private List<String> stringKinds = new ArrayList<String>(); // the inverse association between
        // numerical kinds and human readable strings.

    // Constructors
    /**
     * Constructs a new CommodityTypeFactory. CommoditySpecifications should usually be constructed from the
     * same CommodityTypeFactory instance.
     */
    public CommodityTypeFactory()
    {
        // empty body
    }

    // Methods

    public CommoditySpecification create(String kind, String unitOfMeasurement, int lowerQualityBound, int upperQualityBound) {
        if(!numericalKinds.containsKey(kind)) {
            numericalKinds.put(kind, (short)stringKinds.size());
            stringKinds.add(kind);
        }
        return new CommoditySpecification(kind,unitOfMeasurement,composeNumericalRepresentation(numericalKinds.get(kind),lowerQualityBound,upperQualityBound));
    }
} // end class CommodityTypeFactory
