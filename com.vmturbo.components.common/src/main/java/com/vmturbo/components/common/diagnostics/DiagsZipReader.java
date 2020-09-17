package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.ZipInputStream;

import javax.annotation.Nullable;

import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.components.common.diagnostics.Diags.CompressedDiags;
import com.vmturbo.components.common.diagnostics.Diags.UncompressedDiags;
import com.vmturbo.components.common.diagnostics.RecursiveZipIterator.WrappedZipEntry;

/**
 * This class reads Diags entries from a diags file and creates an iterable that delivers them.
 *
 * <p/>The outermost file is expected to be a ZIP file. File entries are treated based on their
 * file extension, as in:
 * <dl>
 *     <dt>.binary</dt>
 *     <dd>Content is a byte array comprising a binary diags object</dd>
 *     <dt>.diags</dt>
 *     <dd>Content is UTF-8 text comprising a text diags object</dd>
 *     <dt>.zip</dt>
 *     <dd>This is an embedded zip file that is scanned for diags (and further embedded zips), which
 *     are delivered before continuing with this file's diags objects.</dd>
 * </dl>
 * Other entries, including directory entries, are ignored.
 *
 * <p/>If an exception is encountered during this processing, the iterable stops delivering new
 * diags objects, and the exception can be obtained using the {@link #getException} method.
 * Otherwise this case is indistinguishable from coming having fully scanned the input.
 *
 */
public class DiagsZipReader implements Iterable<Diags> {

    /**
     * Files with this suffix should have text/string diagnostic data.
     */
    public static final String TEXT_DIAGS_SUFFIX = ".diags";

    /**
     * Files with this suffix should have binary data.
     */
    public static final String BINARY_DIAGS_SUFFIX = ".binary";

    private final DiagsIterator diagsIterator;

    /**
     * Create a new instance to scan the given input stream.
     *
     * @param inputStream input stream to be scanned
     */
    public DiagsZipReader(InputStream inputStream) {
        this(inputStream, null, false);
    }

    /**
     * Create a new instance to scan the given input stream.
     *
     * @param inputStream Input stream to be scanned.
     * @param customDiagHandler interface that can copy files from the diags into the volumes of
     *                          the components
     * @param compress Whether or not to compress the input stream entry data. This should be "true"
     *                 if you are expecting lots of data, AND you intend to collect the diags
     *                 in memory (e.g. to sort them and process them in a particular way).
     */
    public DiagsZipReader(InputStream inputStream,
                          CustomDiagHandler customDiagHandler,
                          final boolean compress) {
        final RecursiveZipIterator zipIterator = new RecursiveZipIterator(new ZipInputStream(inputStream));
        this.diagsIterator = new DiagsIterator(zipIterator, customDiagHandler, compress);
    }

    @Override
    public Iterator<Diags> iterator() {
        return diagsIterator;
    }

    /**
     * Obtain the exception that caused the zip iterator to terminate early, if that happened.
     *
     * @return iterator-terminating exception, or null if that did not happen
     */
    public Throwable getException() {
        return diagsIterator.getException();
    }

    /**
     * Iterator class supporting the top-level Iterable.
     */
    private static class DiagsIterator implements Iterator<Diags> {

        private RecursiveZipIterator zipIterator;
        private Diags nextValue = null;
        private Throwable exception = null;
        private final boolean compress;
        private final SharedByteBuffer sharedByteBuffer = new SharedByteBuffer(0);
        private final CustomDiagHandler customDiagHandler;

        /**
         * Create a new instance, given a zip file iterator.
         *
         * @param zipIterator iterator to deliver zip file entries
         * @param compress See {@link DiagsZipReader#DiagsZipReader(InputStream)}
         * @param customDiagHandler See {@link DiagsZipReader#DiagsZipReader(InputStream)}
         */
        DiagsIterator(RecursiveZipIterator zipIterator,
                      @Nullable CustomDiagHandler customDiagHandler,
                      final boolean compress) {
            this.zipIterator = zipIterator;
            this.compress = compress;
            this.customDiagHandler = customDiagHandler;
            // tee up first value for delivery
            produceNextValue();
        }

        @Override
        public boolean hasNext() {
            return nextValue != null;
        }

        @Override
        public Diags next() {
            if (nextValue != null) {
                try {
                    return nextValue;
                } finally {
                    // tee up next value to deliver, while returning current value
                    produceNextValue();
                }
            } else {
                throw new NoSuchElementException();
            }
        }

        private void produceNextValue() {
            nextValue = null;
            while (nextValue == null && zipIterator.hasNext()) {
                // Get next entry from zip file (or recursively embedded zip file)
                WrappedZipEntry nextEntry = zipIterator.next();
                if (nextEntry.isExceptionEntry()) {
                    // exception reported by zip iterator, save exception and terminate iterator
                    handleException(nextEntry.getException());
                    return;
                }
                if (customDiagHandler != null && customDiagHandler.shouldHandleRestore(nextEntry)) {
                    customDiagHandler.restore(nextEntry);
                    continue;
                }

                if (isDiagsEntry(nextEntry)) {
                    try {
                        // parse the diags entry and stage it for delivery
                        nextValue = toDiags(nextEntry);
                    } catch (IOException e) {
                        // failed to parse diags entry - terminate the iterator early
                        handleException(e);
                        return;
                    }
                }
            }
            if (nextValue == null) {
                // tie off zip stream once we've gotten everything
                zipIterator.close();
            }
        }

        /**
         * Check whether the given zip file entry looks like a diags entry
         *
         * <p>Any normal file entry with one of our recognized extensions qualiifies.</p>
         *
         * @param entry the entryt to be checked
         * @return true if it appears to be a diags entry
         */
        private boolean isDiagsEntry(WrappedZipEntry entry) {
            return !entry.isExceptionEntry() && !entry.isDirectory()
                && (Diags.isTextDiags(entry.getName()) || Diags.isBinaryDiags(entry.getName()));
        }

        /**
         * Create a {@link Diags} object from the given zip file entry.
         *
         * @param entry the zip entry
         * @return the parsed Diags object
         * @throws IOException if the conversion fails
         */
        private Diags toDiags(WrappedZipEntry entry) throws IOException {
            final byte[] content = entry.getContent();
            try {
                if (compress) {
                    return new CompressedDiags(entry.getName(), content, sharedByteBuffer);
                } else {
                    return new UncompressedDiags(entry.getName(), content);
                }
            } catch (RuntimeException e) {
                handleException(e);
                return null;
            }
        }

        /**
         * Shut down the iterator and stash the exception that's the casue.
         *
         * @param e exception causing the iterator to terminate early
         */
        private void handleException(Throwable e) {
            this.exception = e;
            // close the zip file iterator and make sure we don't try to use it again
            zipIterator.close();
            this.zipIterator = null;
        }

        /**
         * Return the exception that caused this iterator to terminate early.
         *
         * @return the terminating exception, or null if that didn't happen
         */
        public Throwable getException() {
            return exception;
        }
    }
}
