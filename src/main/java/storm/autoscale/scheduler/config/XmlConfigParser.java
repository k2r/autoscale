/**
 * 
 */
package storm.autoscale.scheduler.config;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author Roland
 *
 */
public class XmlConfigParser {
	
	/*Launch file parameters*/
	private String filename;
	private final DocumentBuilderFactory factory;
	private final DocumentBuilder builder;
	private final Document document;
	
	/*Storm parameters*/
	private String nimbusHost;
	private Integer nimbusPort;
	
	/*Monitoring parameters*/
	private Integer monitoringFrequency;
	private Integer windowSize;
	private Double alpha;
	private Double stabilityThreshold;
	private Double highActivityThreshold;
	private Double graceCoeff;
	private Double slopeThreshold;
	
	/*Database parameters*/
	private String dbHost;
	private String dbName;
	private String dbUser;
	private String dbPassword;
	
	public XmlConfigParser(String filename) throws ParserConfigurationException, SAXException, IOException{
		this.filename = filename;
		this.factory = DocumentBuilderFactory.newInstance();
		this.builder = factory.newDocumentBuilder();
		this.document = builder.parse(this.getFilename());
	}

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

	/**
	 * @return the document
	 */
	public Document getDocument() {
		return document;
	}

	/**
	 * @return the nimbusHost
	 */
	public String getNimbusHost() {
		return nimbusHost;
	}

	/**
	 * @param nimbusHost the nimbusHost to set
	 */
	public void setNimbusHost(String nimbusHost) {
		this.nimbusHost = nimbusHost;
	}

	/**
	 * @return the nimbusPort
	 */
	public Integer getNimbusPort() {
		return nimbusPort;
	}

	/**
	 * @param nimbusPort the nimbusPort to set
	 */
	public void setNimbusPort(Integer nimbusPort) {
		this.nimbusPort = nimbusPort;
	}

	/**
	 * @return the monitoringFrequency
	 */
	public Integer getMonitoringFrequency() {
		return monitoringFrequency;
	}

	/**
	 * @param monitoringFrequency the monitoringFrequency to set
	 */
	public void setMonitoringFrequency(Integer monitoringFrequency) {
		this.monitoringFrequency = monitoringFrequency;
	}

	/**
	 * @return the windowSize
	 */
	public Integer getWindowSize() {
		return windowSize;
	}

	/**
	 * @param windowSize the windowSize to set
	 */
	public void setWindowSize(Integer windowSize) {
		this.windowSize = windowSize;
	}

	/**
	 * @return the alpha
	 */
	public Double getAlpha() {
		return alpha;
	}

	/**
	 * @param alpha the alpha to set
	 */
	public void setAlpha(Double alpha) {
		this.alpha = alpha;
	}

	/**
	 * @return the stabilityThreshold
	 */
	public Double getStabilityThreshold() {
		return stabilityThreshold;
	}

	/**
	 * @param stabilityThreshold the stabilityThreshold to set
	 */
	public void setStabilityThreshold(Double stabilityThreshold) {
		this.stabilityThreshold = stabilityThreshold;
	}

	/**
	 * @return the highActivityThreshold
	 */
	public Double getHighActivityThreshold() {
		return highActivityThreshold;
	}

	/**
	 * @param highActivityThreshold the highActivityThreshold to set
	 */
	public void setHighActivityThreshold(Double highActivityThreshold) {
		this.highActivityThreshold = highActivityThreshold;
	}

	/**
	 * @return the graceCoeff
	 */
	public Double getGraceCoeff() {
		return graceCoeff;
	}

	/**
	 * @param graceCoeff the graceCoeff to set
	 */
	public void setGraceCoeff(Double stabilizationCoeff) {
		this.graceCoeff = stabilizationCoeff;
	}

	/**
	 * @return the slopeThreshold
	 */
	public Double getSlopeThreshold() {
		return slopeThreshold;
	}

	/**
	 * @param slopeThreshold the slopeThreshold to set
	 */
	public void setSlopeThreshold(Double slopeThreshold) {
		this.slopeThreshold = slopeThreshold;
	}

	/**
	 * @return the dbHost
	 */
	public String getDbHost() {
		return dbHost;
	}

	/**
	 * @param dbHost the dbHost to set
	 */
	public void setDbHost(String dbHost) {
		this.dbHost = dbHost;
	}

	/**
	 * @return the dbName
	 */
	public String getDbName() {
		return dbName;
	}

	/**
	 * @param dbName the dbName to set
	 */
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	/**
	 * @return the dbUser
	 */
	public String getDbUser() {
		return dbUser;
	}

	/**
	 * @param dbUser the dbUser to set
	 */
	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	/**
	 * @return the dbPassword
	 */
	public String getDbPassword() {
		return dbPassword;
	}

	/**
	 * @param dbPassword the dbPassword to set
	 */
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public void initParameters() {
		Document doc = this.getDocument();
		final Element parameters = (Element) doc.getElementsByTagName(ParameterNames.PARAM.toString()).item(0);
		final NodeList nimbHost = parameters.getElementsByTagName(ParameterNames.NIMBHOST.toString());
		this.setNimbusHost(nimbHost.item(0).getTextContent());
		final NodeList nimbPort = parameters.getElementsByTagName(ParameterNames.NIMBPORT.toString());
		this.setNimbusPort(Integer.parseInt(nimbPort.item(0).getTextContent()));
		final NodeList monitFreq = parameters.getElementsByTagName(ParameterNames.MONITFREQ.toString());
		this.setMonitoringFrequency(Integer.parseInt(monitFreq.item(0).getTextContent()));
		final NodeList winSize = parameters.getElementsByTagName(ParameterNames.WINSIZE.toString());
		this.setWindowSize(Integer.parseInt(winSize.item(0).getTextContent()));
		final NodeList alphaBalancing = parameters.getElementsByTagName(ParameterNames.ALPHA.toString());
		this.setAlpha(Double.parseDouble(alphaBalancing.item(0).getTextContent()));
		final NodeList stab = parameters.getElementsByTagName(ParameterNames.STABIL.toString());
		this.setStabilityThreshold(Double.parseDouble(stab.item(0).getTextContent()));
		final NodeList highAct = parameters.getElementsByTagName(ParameterNames.HIGHACT.toString());
		this.setHighActivityThreshold(Double.parseDouble(highAct.item(0).getTextContent()));
		final NodeList graceCoeff = parameters.getElementsByTagName(ParameterNames.GRACECOEFF.toString());
		this.setGraceCoeff(Double.parseDouble(graceCoeff.item(0).getTextContent()));
		final NodeList slope = parameters.getElementsByTagName(ParameterNames.SLOPE.toString());
		this.setSlopeThreshold(Double.parseDouble(slope.item(0).getTextContent()));
		final NodeList dbHost = parameters.getElementsByTagName(ParameterNames.DBHOST.toString());
		this.setDbHost(dbHost.item(0).getTextContent());
		final NodeList dbName = parameters.getElementsByTagName(ParameterNames.DBNAME.toString());
		this.setDbName(dbName.item(0).getTextContent());
		final NodeList dbUser = parameters.getElementsByTagName(ParameterNames.DBUSER.toString());
		this.setDbUser(dbUser.item(0).getTextContent());
		final NodeList dbPwd = parameters.getElementsByTagName(ParameterNames.DBPWD.toString());
		this.setDbPassword(dbPwd.item(0).getTextContent());
	}
}
