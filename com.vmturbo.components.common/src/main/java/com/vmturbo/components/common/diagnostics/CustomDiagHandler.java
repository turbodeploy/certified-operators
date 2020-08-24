package com.vmturbo.components.common.diagnostics;

import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.diagnostics.RecursiveZipIterator.WrappedZipEntry;

/**
 * Used to handle restoring and dumping operations that interact with diags. Given a diags
 * zipstream this interface supports recognizing if the diag entry should be restored and
 * supports restoring it. In addition it also supports writing some objects in the
 * {@link ZipOutputStream}.
 */
public interface CustomDiagHandler {

     /**
      * Whether a zip entry should be restored from the diags.
      *
      * @param zipEntry entry to copy
      * @return whether the file should be copied or not
      */
     boolean shouldHandleRestore(WrappedZipEntry zipEntry);


     /**
      * Restore the zip entry from the diags.
      *
      * @param zipEntry entry to copy
      */
     void restore(WrappedZipEntry zipEntry);

     /**
      * Dump zip entries in the passed {@link ZipOutputStream}.
      *
      * @param zipStream stream to append zip entries to
      */
     void dumpToStream(@Nonnull ZipOutputStream zipStream);

}
