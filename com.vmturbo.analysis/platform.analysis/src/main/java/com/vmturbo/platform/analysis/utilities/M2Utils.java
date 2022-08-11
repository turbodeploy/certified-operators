package com.vmturbo.platform.analysis.utilities;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.vmturbo.platform.analysis.topology.LegacyTopology;

public class M2Utils {
    public static void main(String... args) {
        try {
            loadFile(args[0]);
        } catch (IOException | ParseException | ParserConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Load the specified file. The file is an EMF repository file saved by an Operations Manager.
     * This method uses a static {@link Logger} which name is the {@link EMF2MarketHandler} class name.
     * @param fileName the name of the file to load
     * @return a {@link LegacyTopology} that contains a market2 representation of the entities in the file
     */
    public static LegacyTopology loadFile(String fileName)
            throws IOException, ParserConfigurationException, ParseException {
        try (FileInputStream input = new FileInputStream(fileName)) {
            return loadStream(input);
        }
    }

    /**
     * Load the specified file. The file is an EMF repository file saved by an Operations Manager.
     * @param fileName the name of the file to load
     * @param logger the {@link Logger} to be used when parsing the file by the {@link EMF2MarketHandler}
     * SAX handler.
     * @return a {@link LegacyTopology} that contains a market2 representation of the entities in the file
     */
    public static LegacyTopology loadFile(String fileName, Logger logger)
            throws IOException, ParserConfigurationException, ParseException {
        try (FileInputStream input = new FileInputStream(fileName)) {
            return loadStream(input, logger);
        }
    }

    /**
     * Load an {@link InputStream} in the format of a file saved by an OperationsManager.
     * Use a static {@link Logger} which name is the {@link EMF2MarketHandler} class name.
     * @param stream the InputStream to load
     * @return a {@link LegacyTopology} that contains a market2 representation of the entities in the file
     */
    public static LegacyTopology loadStream(InputStream stream)
            throws IOException, ParserConfigurationException, ParseException {
        return loadStream(stream, Logger.getLogger(EMF2MarketHandler.class));
    }

    /**
     * Load an {@link InputStream} in the format of a file saved by an OperationsManager.
     * @param stream the InputStream to load
     * @param logger the {@link Logger} to be used when parsing the stream by the {@link EMF2MarketHandler}
     * SAX handler.
     * @return a {@link LegacyTopology} that contains a market2 representation of the entities in the stream
     */
    public static LegacyTopology loadStream(InputStream stream, Logger logger)
            throws IOException, ParserConfigurationException, ParseException {
        EMF2MarketHandler handler = new EMF2MarketHandler(logger);
        try {
            SAXParserFactory.newInstance().newSAXParser().parse(stream, handler);
        }
        catch (SAXException e) {
            // Wrap the SAX specific exception in a standard library one so we can change our choice
            // of parser in the future without changing the method signature.
            throw (ParseException)new ParseException("The internal XML parser signaled an error!",
                // TODO: not sure how to get the offset argument needed by ParseException, using the
                // line number should be enough for now and arguably more useful!
                e instanceof SAXParseException ? ((SAXParseException)e).getLineNumber() : 0).initCause(e);
        }
        return handler.getTopology();
    }

}
