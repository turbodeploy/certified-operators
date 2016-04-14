package com.vmturbo.platform.analysis.utilities;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.vmturbo.platform.analysis.topology.LegacyTopology;

public class M2Utils {
    public static void main(String... args) {
        try {
            loadFile(args[0]);
        } catch (FileNotFoundException e) {
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
    public static LegacyTopology loadFile(String fileName) throws FileNotFoundException {
        return loadStream(new FileInputStream(fileName));
    }

    /**
     * Load the specified file. The file is an EMF repository file saved by an Operations Manager.
     * @param fileName the name of the file to load
     * @param logger the {@link Logger} to be used when parsing the file by the {@link EMF2MarketHandler}
     * SAX handler.
     * @return a {@link LegacyTopology} that contains a market2 representation of the entities in the file
     */
    public static LegacyTopology loadFile(String fileName, Logger logger) throws FileNotFoundException {
        return loadStream(new FileInputStream(fileName), logger);
    }

    /**
     * Load an {@link InputStream} in the format of a file saved by an OperationsManager.
     * Use a static {@link Logger} which name is the {@link EMF2MarketHandler} class name.
     * @param stream the InputStream to load
     * @return a {@link LegacyTopology} that contains a market2 representation of the entities in the file
     */
    public static LegacyTopology loadStream(InputStream stream) {
        return loadStream(stream, Logger.getLogger(EMF2MarketHandler.class));
    }

    /**
     * Load an {@link InputStream} in the format of a file saved by an OperationsManager.
     * @param stream the InputStream to load
     * @param logger the {@link Logger} to be used when parsing the stream by the {@link EMF2MarketHandler}
     * SAX handler.
     * @return a {@link LegacyTopology} that contains a market2 representation of the entities in the stream
     */
    public static LegacyTopology loadStream(InputStream stream, Logger logger) {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        try {
            SAXParser parser = factory.newSAXParser();
            EMF2MarketHandler handler = new EMF2MarketHandler(logger);
            parser.parse(stream, handler);
            return handler.getTopology();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
