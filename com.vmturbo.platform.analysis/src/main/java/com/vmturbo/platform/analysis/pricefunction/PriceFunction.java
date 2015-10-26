package com.vmturbo.platform.analysis.pricefunction;

public interface PriceFunction {

	// TODO(Shai): javadoc
    double unitPrice(double normalizedUtilization);
    double unitPrice(double utilization, double utilThreshold);
    double unitPeakPrice(double utilization, double peakUtilization, double utilThreshold);

}
