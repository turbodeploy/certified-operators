package com.vmturbo.stitching;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

/**
 * Cache for external signatures. We should create a new {@link ExternalSignatureCache}
 * for each pipeline run. This allows operations to cache their external signature
 * mappings within a pipeline run, and allows those signatures to be discarded once
 * the run is complete.
 */
public class ExternalSignatureCache {
    private final Map<StitchingOperation<?, ?>,
        Map<?, Collection<StitchingEntity>>> operationSignatureCache = new HashMap<>();

    /**
     * Create a new external signature cache.
     */
    public ExternalSignatureCache() {
        // Nothing to do
    }

    /**
     * Lookup the external signatures for a given stitching operation. If the signatures
     * haven't been created yet, call the supplier to create those signatures and
     * cache them for future use.
     *
     * @param operation The operation whose external signatures should be looked up or
     *                  generated via the supplier.
     * @param signatureSupplier The supplier for the operation's external signatures. Only
     *                          called if the operation's signatures are not yet cached.
     * @param <ExternalSignatureT> The external signature type of the stitching operation.
     * @return The map of external signatures to corresponding external entities for the
     *         stitching operation.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <ExternalSignatureT> Map<ExternalSignatureT, Collection<StitchingEntity>>
    computeSignaturesIfAbsent(@Nonnull final StitchingOperation<?, ExternalSignatureT> operation,
                              @Nonnull final Supplier<Map<ExternalSignatureT,
                                  Collection<StitchingEntity>>> signatureSupplier) {
        // Java's type system isn't powerful enough to be able to express that this is type-safe,
        // but it is because we can only insert into the map through this method and that ensures
        // that the type of the key in the inserted map will match the external signature type of
        // the corresponding operation.
        return (Map<ExternalSignatureT, Collection<StitchingEntity>>)operationSignatureCache
            .computeIfAbsent(operation, op -> signatureSupplier.get());
    }
}
