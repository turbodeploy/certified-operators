package com.vmturbo.matrix.component;

import javax.annotation.Nonnull;

/**
 * The {@link OverlayEdge} implements a weighted communication matrix edge.
 */
public class OverlayEdge extends BaseEdge {
    /**
     * The latency in microseconds.
     */
    private long latency_;

    /**
     * The flow.
     */
    private double flow_;

    /**
     * The transmitted bytes.
     */
    private long tx_;

    /**
     * The received bytes.
     */
    private long rx_;

    /**
     * The accumulated data amount transferred in both directions.
     */
    private long accumulatedData_;

    /**
     * The initialization timestamp.
     */
    private long lastUpdateTimestamp_;

    /**
     * Construct default matrix edge.
     */
    private OverlayEdge() {
    }

    /**
     * Consructs the edge.
     *
     * @param latency The latency in microseconds.
     * @param flow    The flow.
     * @param tx      The transmitted amount in bytes.
     * @param rx      The received amount in bytes.
     * @param source  The source vertex.
     * @param sink    The sink vertex.
     */
    OverlayEdge(final long latency, final double flow,
                final long tx, final long rx,
                final @Nonnull OverlayVertex source,
                final @Nonnull OverlayVertex sink) {
        super(source, sink);
        if (latency < 0f || flow < 0f) {
            throw new IllegalArgumentException("Check latency: " + latency + " and flow: " + flow);
        }
        latency_ = latency & 0xFFFFFFFFL; // Only allow 32 bit of the latency. 1 hour is enough.
        flow_ = flow;
        tx_ = tx;
        rx_ = rx;
        accumulatedData_ = tx_ + rx_;
        lastUpdateTimestamp_ = System.currentTimeMillis();
    }

    /**
     * Checks whether edge has expired.
     *
     * @param interval The interval to check against.
     * @return {@code true} iff the interval since last update is greater or equals to the
     * {@code interval} parameter.
     */
    boolean isExpired(final long interval) {
        return (System.currentTimeMillis() - lastUpdateTimestamp_) >= interval;
    }

    /**
     * Updates the {@link #lastUpdateTimestamp_}.
     */
    void updateTimestamp() {
        lastUpdateTimestamp_ = System.currentTimeMillis();
    }

    /**
     * Sets the flow.
     *
     * @param flow The flow.
     */
    final void setFlow(final double flow) {
        flow_ = flow;
    }

    /**
     * Returns the latency.
     *
     * @return The latency.
     */
    public final long getLatency() {
        return latency_;
    }

    /**
     * Returns the flow.
     *
     * @return The flow.
     */
    public final double getFlow() {
        return flow_;
    }

    /**
     * Retrieves the amount of transmitted bytes.
     *
     * @return The amount of transmitted bytes.
     */
    public long getTx() {
        return tx_;
    }

    /**
     * The amount of received bytes.
     *
     * @return The amount of received bytes.
     */
    public long getRx() {
        return rx_;
    }

    /**
     * Returns the accumulated data amount transferred in both directions.
     *
     * @return The accumulated data amount transferred in both directions.
     */
    public long getAccumulatedData() {
        return accumulatedData_;
    }

    /**
     * Accumulate the data.
     *
     * @param amount The new amount of data being accumulated.
     */
    public void accumulateData(final long amount) {
        accumulatedData_ += amount;
    }

    /**
     * Returns the source vertex.
     *
     * @return The source vertex.
     */
    protected @Nonnull OverlayVertex getSource() {
        return (OverlayVertex)super.getSource();
    }

    /**
     * Returns the sink vertex.
     *
     * @return The sink vertex.
     */
    protected @Nonnull OverlayVertex getSink() {
        return (OverlayVertex)super.getSink();
    }

    /**
     * Creates a new instance.
     *
     * @return The new instance.
     */
    protected @Nonnull BaseEdge newInstance() {
        return new OverlayEdge();
    }

    /**
     * Returns a shallow copy of this edge.
     *
     * @param src The source.
     * @param dst The sink.
     * @return The shallow copy of this edge.
     */
    protected @Nonnull OverlayEdge shallowCopy(final @Nonnull OverlayVertex src,
                                               final @Nonnull OverlayVertex dst) {
        OverlayEdge edge = (OverlayEdge)super.shallowCopy();
        edge.source_ = src;
        edge.sink_ = dst;
        edge.latency_ = latency_;
        edge.flow_ = flow_;
        edge.tx_ = tx_;
        edge.rx_ = rx_;
        edge.accumulatedData_ = accumulatedData_;
        edge.lastUpdateTimestamp_ = lastUpdateTimestamp_;
        return edge;
    }
}
