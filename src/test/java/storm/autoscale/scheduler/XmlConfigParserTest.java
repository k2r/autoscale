/**
 * 
 */
package storm.autoscale.scheduler;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import junit.framework.TestCase;
import storm.autoscale.scheduler.config.XmlConfigParser;

/**
 * @author Roland
 *
 */
public class XmlConfigParserTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getFilename()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetFilename() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		String expected = "./conf/autoscale_parameters.xml";
		assertEquals(expected, parser.getFilename());
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getNimbusHost()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetNimbusHost() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		String expected = "roland-spg";
		assertEquals(expected, parser.getNimbusHost());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getNimbusPort()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetNimbusPort() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		Integer expected = 6627;
		assertEquals(expected, parser.getNimbusPort(), 0);
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getMonitoringFrequency()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetMonitoringFrequency() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		Integer expected = 10;
		assertEquals(expected, parser.getMonitoringFrequency(), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getWindowSize()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetWindowSize() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		Integer expected = 60;
		assertEquals(expected, parser.getWindowSize(), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getLowActivityThreshold()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetLowActivityThreshold() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		Double expected = 0.1;
		assertEquals(expected, parser.getLowActivityThreshold(), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getHighActivityThreshold()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetHighActivityThreshold() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		Double expected = 0.8;
		assertEquals(expected, parser.getHighActivityThreshold(), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getStabilizationCoeff()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetStabilizationCoeff() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		Double expected = 1.0;
		assertEquals(expected, parser.getStabilizationCoeff(), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getSlopeThreshold()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetSlopeThreshold() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		Double expected = 0.1;
		assertEquals(expected, parser.getSlopeThreshold(), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getDbHost()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetDbHost() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		String expected = "localhost";
		assertEquals(expected, parser.getDbHost());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getDbName()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetDbName() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		String expected = "autoscale_test";
		assertEquals(expected, parser.getDbName());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getDbUser()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetDbUser() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		String expected = "root";
		assertEquals(expected, parser.getDbUser());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.config.XmlConfigParser#getDbPassword()}.
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public void testGetDbPassword() throws ParserConfigurationException, SAXException, IOException {
		XmlConfigParser parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
		parser.initParameters();
		String expected = "";
		assertEquals(expected, parser.getDbPassword());
	}

}