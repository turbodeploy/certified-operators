package com.vmturbo.topology.processor.discoverydumper;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;

public class DiscoveryDumpFilenameTest {
    private static final String dateFormatPattern = "yyyy.MM.dd.HH.mm.ss.SSS";
    private void testFilename(String filename, String targetName, Date timeStamp, DiscoveryType discoveryType, boolean mustFail) {
        final DiscoveryDumpFilename ddFileName = DiscoveryDumpFilename.parse(filename);

        if (mustFail) {
            Assert.assertNull(ddFileName);
            return;
        }

        Assert.assertNotNull(ddFileName);
        Assert.assertEquals(
            ddFileName.getSanitizedTargetName(), DiscoveryDumpFilename.sanitize(targetName));
        Assert.assertEquals(timeStamp, ddFileName.getTimestamp());
        Assert.assertEquals(discoveryType, ddFileName.getDiscoveryType());

        String tmpDir = System.getProperty("java.io.tmpdir");
        if (tmpDir != null && !tmpDir.endsWith(File.separator)) {
            tmpDir += File.separator;
        }
        final File dumpDirectory = new File(tmpDir);
        Assert.assertEquals(
            tmpDir + filename.substring(0, filename.length() - 3) + "txt",
            ddFileName.getFile(dumpDirectory, true, false).getAbsolutePath());
        Assert.assertEquals(
            tmpDir + filename.substring(0, filename.length() - 3) + "txt.lz4",
            ddFileName.getFile(dumpDirectory, true, true).getAbsolutePath());
    }

    /**
     * This and following tests check whether parsing a discovery dump filename returns a
     * {@link DiscoveryDumpFilename} object with the correct information, and whether the filenames
     * calculated by that object are correct.
     */
    @Test
    public void testFilenameParsingNothingUnusual() throws ParseException {
        final String aDateRepresentation = "2018.04.09.11.30.02.155";
        final Date aDate = new SimpleDateFormat(dateFormatPattern).parse(aDateRepresentation);

        testFilename(
            "normal.target.name.1-" + aDateRepresentation + "-PERFORMANCE.txt",
            "normal.target.name.1", aDate, DiscoveryType.PERFORMANCE, false);
    }

    @Test
    public void testFilenameParsingTrickyTargetName() throws ParseException {
        final String aDateRepresentation = "2018.04.09.11.30.02.155";
        final Date aDate = new SimpleDateFormat(dateFormatPattern).parse(aDateRepresentation);

        testFilename(
            "http___tricky.url.com-" + aDateRepresentation + "-PERFORMANCE.txt",
            "http://tricky.url.com", aDate, DiscoveryType.PERFORMANCE, false);

    }

    @Test
    public void testFilenameParsingSingleUnderscoreTargetName() throws ParseException {
        final String aDateRepresentation = "2018.04.09.11.30.02.155";
        final Date aDate = new SimpleDateFormat(dateFormatPattern).parse(aDateRepresentation);

        testFilename(
            "_-" + aDateRepresentation + "-PERFORMANCE.txt",
            "_", aDate, DiscoveryType.PERFORMANCE, false);

    }

    @Test
    public void testFilenameParsingTargetNameWithSpecialChars() throws ParseException {
        final String aDateRepresentation = "2018.04.09.11.30.02.155";
        final Date aDate = new SimpleDateFormat(dateFormatPattern).parse(aDateRepresentation);

        testFilename(
            "A_B_-" + aDateRepresentation + "-PERFORMANCE.txt",
            "A/BÀ", aDate, DiscoveryType.PERFORMANCE, false);

    }


    @Test
    public void testFilenameParsingSpecialCharsFull() throws ParseException {
        final String aDateRepresentation = "2018.04.09.11.30.02.155";
        final Date aDate = new SimpleDateFormat(dateFormatPattern).parse(aDateRepresentation);

        testFilename(
            "A_B_-" + aDateRepresentation + "-FULL.txt",
            "A BÀ", aDate, DiscoveryType.FULL, false);

    }

    /**
     * This and following test perform checks similar to the above, but each one
     * presents a name that should cause the parser to fail to recognize this as
     * a dump file.
     */
    @Test
    public void testFilenameParsingEmptyTargetName() {
        final String aDateRepresentation = "2018.04.09.11.30.02.155";

        testFilename(
            "-" + aDateRepresentation + "-PERFORMANCE.txt",
            "", null, null, true);

    }

    @Test
    public void testFilenameParsingInvalidDiscoveryType() {
        final String aDateRepresentation = "2018.04.09.11.30.02.155";

        testFilename(
            "A-" + aDateRepresentation + "-PERFORMANC.txt",
            "A", null, null, true);

    }

    @Test
    public void testFilenameParsingTopologyFileRejected() {
        final String aDateRepresentation = "2018.04.09.11.30.02.155";

        testFilename(
            "A_B_-" + aDateRepresentation + ".topology",
            "A BÀ", null, null, true);
    }
}
