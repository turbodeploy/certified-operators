package com.vmturbo.platform.analysis.utilities;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import com.vmturbo.platform.analysis.topology.Topology;

public class M2Utils {
    public static void main(String... args) {
        loadFile(args[0]);
    }

    public static Topology loadFile(String fileName) {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        try {
            SAXParser parser = factory.newSAXParser();
            EMF2MarketHandler handler = new EMF2MarketHandler();
            parser.parse(fileName, handler);
            return handler.getTopology();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
