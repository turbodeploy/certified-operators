package com.vmturbo.extractor.export;

import com.vmturbo.components.api.chunking.ChunkCollector;
import com.vmturbo.extractor.schema.json.export.ExportedObject;

/**
 * Utility to collect list of exported objects into chunks, respecting size (in bytes) limits.
 */
public class ExportedObjectChunkCollector extends ChunkCollector<ExportedObject> {

    /**
     * Create a new instance of the chunk collector.
     *
     * @param desiredSizeBytes The desired size of a chunk. The collector will try to size each
     *                         chunk to this amount without splitting up individual elements.
     *                         e.g. if desiredSize is 100 bytes and we get three elements of 30,
     *                         80, and 60 bytes, we will put the first two into one chunk, and the
     *                         next into another chunk.
     * @param maxSizeBytes The maximum size of a chunk.
     */
    public ExportedObjectChunkCollector(final int desiredSizeBytes, final int maxSizeBytes) {
        super(desiredSizeBytes, maxSizeBytes);
    }

    @Override
    public int getSerializedSize(ExportedObject element) {
        return element.getSerializedSize();
    }
}
