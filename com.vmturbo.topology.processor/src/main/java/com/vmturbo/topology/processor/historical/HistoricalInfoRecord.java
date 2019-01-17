package com.vmturbo.topology.processor.historical;

import javax.annotation.Nonnull;

/**
 *  Helper class for storing the historical info.
 *  The info saved are the peak and used values of the sold and bought
 *  commodities for each service entity as well as the weights used
 *  to calculate the peak and used values. The info are saved as a blob.
 */
public class HistoricalInfoRecord {

    private byte[] info = null;

    public HistoricalInfoRecord() {
    }

    public HistoricalInfoRecord(@Nonnull byte[] info) {
        this.info = info;
    }

    public byte[] getInfo() {return info;}

    public void setInfo(byte[] info) { this.info = info; }
}
