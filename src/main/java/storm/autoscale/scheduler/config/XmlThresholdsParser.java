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
public class XmlThresholdsParser {

	/*Launch file parameters*/
	private String filename;
	private final DocumentBuilderFactory factory;
	private final DocumentBuilder builder;
	private final Document document;
	
	/*Thresholds*/
	private Integer delta;
	private Double activityMin;
	private Double activityMax;
	
	public XmlThresholdsParser(String filename) throws ParserConfigurationException, SAXException, IOException{
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
	 * @return the delta
	 */
	public Integer getDelta() {
		return delta;
	}
	/**
	 * @param delta the delta to set
	 */
	public void setDelta(Integer delta) {
		this.delta = delta;
	}
	
	/**
	 * @return the activityMin
	 */
	public Double getActivityMin() {
		return activityMin;
	}
	/**
	 * @param activityMin the activityMin to set
	 */
	public void setActivityMin(Double activityMin) {
		this.activityMin = activityMin;
	}
	/**
	 * @return the activityMax
	 */
	public Double getActivityMax() {
		return activityMax;
	}
	/**
	 * @param activityMax the activityMax to set
	 */
	public void setActivityMax(Double activityMax) {
		this.activityMax = activityMax;
	}
	/**
	 * @return the factory
	 */
	public DocumentBuilderFactory getFactory() {
		return factory;
	}
	/**
	 * @return the builder
	 */
	public DocumentBuilder getBuilder() {
		return builder;
	}
	/**
	 * @return the document
	 */
	public Document getDocument() {
		return document;
	}
	
	public void initParameters() {
		Document doc = this.getDocument();
		final Element parameters = (Element) doc.getElementsByTagName(ParameterNames.PARAM.toString()).item(0);
		final NodeList delta = parameters.getElementsByTagName(ParameterNames.DELTA.toString());
		this.setDelta(Integer.parseInt(delta.item(0).getTextContent()));
		final NodeList activityMin = parameters.getElementsByTagName(ParameterNames.ACTMIN.toString());
		this.setActivityMin(Double.parseDouble(activityMin.item(0).getTextContent()));
		final NodeList activityMax = parameters.getElementsByTagName(ParameterNames.ACTMAX.toString());
		this.setActivityMax(Double.parseDouble(activityMax.item(0).getTextContent()));
	}
	
}