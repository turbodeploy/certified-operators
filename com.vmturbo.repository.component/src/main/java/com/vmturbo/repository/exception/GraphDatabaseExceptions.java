package com.vmturbo.repository.exception;

/**
 * A list of possible exceptions return while creating database graphs.
 */
public class GraphDatabaseExceptions {
    public static class GraphDatabaseException extends Exception {
        public GraphDatabaseException(final String msg) {
            super(msg);
        }

        public GraphDatabaseException(final String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public static class GraphOperationException extends GraphDatabaseException {
        public GraphOperationException(final String msg) {
            super(msg);
        }

        public GraphOperationException(final String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public static class VertexOperationException extends GraphDatabaseException {
        public VertexOperationException(final String msg) {
            super(msg);
        }

        public VertexOperationException(final String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public static class EdgeOperationException extends GraphDatabaseException {
        public EdgeOperationException(final String msg) {
            super(msg);
        }

        public EdgeOperationException(final String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public static class CollectionOperationException extends GraphDatabaseException {
        public CollectionOperationException(final String msg) {
            super(msg);
        }

        public CollectionOperationException(final String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }

    public static class IndexOperationException extends GraphDatabaseException {
        public IndexOperationException(final String msg) {
            super(msg);
        }

        public IndexOperationException(final String msg, Throwable throwable) {
            super(msg, throwable);
        }
    }
}
