package com.vmturbo.matrix.component;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * The {@link BaseEdge} represents a base edge in a multi-layered graph.
 */
abstract class BaseEdge {
    /**
     * The source vertex.
     */
    protected BaseVertex source_;

    /**
     * The sink vertex.
     */
    protected BaseVertex sink_;

    /**
     * The hash code.
     */
    private int hash_;

    /**
     * Construct default matrix edge.
     */
    BaseEdge() {
    }

    /**
     * Constructs the edge.
     *
     * @param source The source vertex.
     * @param sink   The sink vertex.
     */
    BaseEdge(final @Nonnull OverlayVertex source,
             final @Nonnull OverlayVertex sink) {
        source_ = Objects.requireNonNull(source);
        sink_ = Objects.requireNonNull(sink);
        hash_ = Objects.hash(source_, sink_);
    }

    /**
     * Returns the source vertex.
     *
     * @return The source vertex.
     */
    protected @Nonnull BaseVertex getSource() {
        return source_;
    }

    /**
     * Returns the sink vertex.
     *
     * @return The sink vertex.
     */
    protected @Nonnull BaseVertex getSink() {
        return sink_;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return hash_;
    }

    /**
     * Creates a new instance.
     *
     * @return The new instance.
     */
    protected abstract @Nonnull BaseEdge newInstance();

    /**
     * Returns a shallow copy of this edge.
     *
     * @return The shallow copy of this edge.
     */
    protected @Nonnull BaseEdge shallowCopy() {
        BaseEdge edge = newInstance();
        edge.source_ = source_.shallowCopy();
        edge.sink_ = sink_.shallowCopy();
        edge.hash_ = hash_;
        return edge;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null) {
            return false;
        } else if (getClass() == o.getClass()) {
            BaseEdge other = (BaseEdge)o;
            return source_.equals(other.source_) && sink_.equals(other.sink_);
        } else {
            return false;
        }
    }
}
