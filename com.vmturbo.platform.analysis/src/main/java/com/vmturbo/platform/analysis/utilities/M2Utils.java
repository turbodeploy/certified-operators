package com.vmturbo.platform.analysis.utilities;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import com.vmturbo.platform.analysis.topology.Topology;

public class M2Utils {
	public static void main(String... args) {
		// Large file: 18040 traders / 207481 commodities (bought and sold)
		// loadFile("/Users/shai/VMTurbo/Customers/Premier/bkp-premierinc.com-10.32.2.117-2015-09-30-145058/srv/tomcat/data/repos/customer.repos.topology");
		// Smaller file: 1523 traders / 15376 commodities (bought and sold)
		loadFile("/Users/shai/VMTurbo/Customers/MGH/bkp-mwhglobal.com-172.24.74.7-2015-09-29-153518/srv/tomcat/data/repos/customer.markets.topology");
//		loadFile("/Users/shai/VMTurbo/temp/a.t");
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
