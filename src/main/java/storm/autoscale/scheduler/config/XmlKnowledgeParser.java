/**
 * 
 */
package storm.autoscale.scheduler.config;

import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import storm.autoscale.scheduler.config.knowledge.KnowledgeRule;

/**
 * @author Roland
 *
 */
public class XmlKnowledgeParser {
	
	/*Launch file parameters*/
	private String filename;
	private final DocumentBuilderFactory factory;
	private final DocumentBuilder builder;
	private final Document document;
	
	/*Rules*/
	ArrayList<KnowledgeRule> rules;
	
	public XmlKnowledgeParser(String filename) throws ParserConfigurationException, SAXException, IOException {
		this.filename = filename;
		this.factory = DocumentBuilderFactory.newInstance();
		this.builder = factory.newDocumentBuilder();
		this.document = builder.parse(this.getFilename());
	}
	
	
	/**
	 * @return the filename
	 */
	public final String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public final void setFilename(String filename) {
		this.filename = filename;
	}

	/**
	 * @return the factory
	 */
	public final DocumentBuilderFactory getFactory() {
		return factory;
	}

	/**
	 * @return the builder
	 */
	public final DocumentBuilder getBuilder() {
		return builder;
	}

	/**
	 * @return the document
	 */
	public final Document getDocument() {
		return document;
	}

	/**
	 * @return the rules
	 */
	public final ArrayList<KnowledgeRule> getRules() {
		return rules;
	}
	
	public void initParameters() {
		Document doc = this.getDocument();
		final Element rules = (Element) doc.getElementsByTagName(ParameterNames.RULES.toString()).item(0);
		final NodeList ruleList = rules.getElementsByTagName(ParameterNames.RULE.toString());
		int nbRules = ruleList.getLength();
		for(int i = 0; i < nbRules; i++){
			Double lb = 0.0;
			Double ub = 0.0;
			Integer deg = 1;
			Double rwd = 0.0;
			
			final Element rule = (Element) rules.getElementsByTagName(ParameterNames.RULE.toString()).item(i);
			final NodeList state = rule.getElementsByTagName(ParameterNames.STATE.toString());
			String[] stateToString = state.item(0).getTextContent().split(";");
			lb = Double.parseDouble(stateToString[0]);
			ub = Double.parseDouble(stateToString[1]);
			final NodeList degree = rule.getElementsByTagName(ParameterNames.DEG.toString());
			deg = Integer.parseInt(degree.item(0).getTextContent());
			final NodeList reward = rule.getElementsByTagName(ParameterNames.RWD.toString());
			rwd = Double.parseDouble(reward.item(0).getTextContent());
			KnowledgeRule krule = new KnowledgeRule(lb, ub, deg, rwd);
			this.rules.add(krule);
		}
	}
	
	public Integer bestDegree(Double rate){
		Integer degree = 0;
		for(KnowledgeRule rule : this.rules){
			if(rule.isApplicable(rate)){
				degree = rule.getDegree();
				break;
			}
		}
		return degree;
	}
}
