package com.vmturbo.cost.component.stores;

import com.vmturbo.components.common.diagnostics.BinaryDiagnosable;
import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;

/**
 * Diagnosable single field data store.
 *
 * @param <T> type of data.
 */
public interface DiagnosableSingleFieldDataStore<T>
        extends SingleFieldDataStore<T>, BinaryDiagnosable, BinaryDiagsRestorable<Void> {}
