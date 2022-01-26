package com.vmturbo.cost.component.stores;

import com.vmturbo.components.common.diagnostics.BinaryDiagnosable;
import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;

/**
 * Diagnosable single field data store.
 */
public interface DiagnosableDataStoreCollector extends BinaryDiagnosable, BinaryDiagsRestorable<Void> {}
