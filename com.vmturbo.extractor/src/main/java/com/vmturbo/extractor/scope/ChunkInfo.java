package com.vmturbo.extractor.scope;

import java.sql.Timestamp;

/**
 * POJO class to hold information about a hypertable chunk.
 */
public class ChunkInfo {
    private String chunkSchema = null;
    private String chunkName = null;
    private Timestamp rangeStart = null;
    private Timestamp rangeEnd = null;

    /**
     * Constructor.
     *
     * @param chunk_schema chunk schema
     * @param chunk_name chunk name
     * @param range_start range start
     * @param range_end range end
     */
    public ChunkInfo(String chunk_schema, String chunk_name, Timestamp range_start, Timestamp range_end) {
        this.chunkSchema = chunk_schema;
        this.chunkName = chunk_name;
        this.rangeStart = range_start;
        this.rangeEnd = range_end;
    }

    /**
     * Get chunk schema.
     *
     * @return chunk schema
     */
    public String getChunkSchema() {
        return chunkSchema;
    }

    /**
     * Get chunk name.
     *
     * @return chunk name
     */
    public String getChunkName() {
        return chunkName;
    }

    /**
     * Get range start timestamp.
     *
     * @return range start timestamp
     */
    public Timestamp getRangeStart() {
        return rangeStart;
    }

    /**
     * Get range end timestamp.
     *
     * @return range end timestamp
     */
    public Timestamp getRangeEnd() {
        return rangeEnd;
    }

    @Override
    public String toString() {
        return "ChunkInfo{"
                + "chunkSchema='" + chunkSchema + '\''
                + ", chunkName='" + chunkName + '\''
                + ", rangeStart=" + rangeStart
                + ", rangeEnd=" + rangeEnd
                + '}';
    }
}
