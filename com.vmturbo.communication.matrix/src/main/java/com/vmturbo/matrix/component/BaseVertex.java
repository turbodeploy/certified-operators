package com.vmturbo.matrix.component;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * The {@link BaseVertex} represents a basic vertex in a multi-layered graph.
 */
abstract class BaseVertex {
    /**
     * The id.
     */
    private ID id_;

    /**
     * The hash code.
     */
    private int hash_;

    /**
     * Initializes the node.
     *
     * @param id The ID.
     */
    protected final void init(final @Nonnull ID id) {
        id_ = id;
        hash_ = id_.hashCode();
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
            final BaseVertex other = (BaseVertex)o;
            return Objects.equals(id_, other.id_);
        } else {
            return false;
        }
    }

    /**
     * Returns a new instance of the Node or its derived class.
     *
     * @return A new instance of the Node or its derived class.
     */
    protected abstract @Nonnull BaseVertex newInstance();

    /**
     * Returns the a shallow copy of this vertex.
     *
     * @return The shallow copy of a vertex.
     */
    @Nonnull BaseVertex shallowCopy() {
        BaseVertex node = newInstance();
        node.hash_ = hash_;
        node.id_ = id_;
        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return hash_;
    }

    /**
     * The opaque ID.
     */
    protected abstract static class ID {
    }
}
