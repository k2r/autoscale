/**
 * 
 */
package storm.autoscale.scheduler.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
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
	
	private Logger logger = Logger.getLogger("XmlKnowledgeParser");
	
	public XmlKnowledgeParser(String filename) throws ParserConfigurationException, SAXException, IOException {
		this.filename = filename;
		this.factory = DocumentBuilderFactory.newInstance();
		this.builder = factory.newDocumentBuilder();
		this.document = builder.parse(this.getFilename());
		this.rules = new ArrayList<>();
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
	
	public void createOrUpdateRule(KnowledgeRule rule){
		String operator = rule.getOperator();
		String state = rule.getLowerBound() + ";" + rule.getUpperBound();
		String degree = rule.getDegree().toString();
		String reward = rule.getReward().toString();
		
		Document doc = this.getDocument();
		final Element rules = (Element) doc.getElementsByTagName(ParameterNames.RULES.toString()).item(0);
		final NodeList ruleList = rules.getElementsByTagName(ParameterNames.RULE.toString());
		int nbRules = ruleList.getLength();
		boolean existRule = false;
		for(int i = 0; i < nbRules; i++){
			
			final Element ruleNode = (Element) rules.getElementsByTagName(ParameterNames.RULE.toString()).item(i);
			final NodeList operatorNode = ruleNode.getElementsByTagName(ParameterNames.OP.toString());
			String op = operatorNode.item(0).getTextContent();
			final NodeList stateNode = ruleNode.getElementsByTagName(ParameterNames.STATE.toString());
			String stateToString = stateNode.item(0).getTextContent();
			final NodeList degreeNode = ruleNode.getElementsByTagName(ParameterNames.DEG.toString());
			final NodeList rewardNode = ruleNode.getElementsByTagName(ParameterNames.RWD.toString());
			Double rwd = Double.parseDouble(rewardNode.item(0).getTextContent());
			
			if(operator.equalsIgnoreCase(op) && state.equalsIgnoreCase(stateToString) && rule.getReward() > rwd && rule.getReward() <= 1 && !rule.getReward().isInfinite() && !rule.getReward().isNaN()){
				degreeNode.item(0).setTextContent(degree);
				rewardNode.item(0).setTextContent(reward);
				existRule = true;
				break;
			}
		}
		
		if(!existRule){
			Node ruleNode = doc.createElement(ParameterNames.RULE.toString());
			Node operatorNode = doc.createElement(ParameterNames.OP.toString());
			operatorNode.setTextContent(operator);
			Node stateNode = doc.createElement(ParameterNames.STATE.toString());
			stateNode.setTextContent(state);
			Node degreeNode = doc.createElement(ParameterNames.DEG.toString());
			degreeNode.setTextContent(degree);
			Node rewardNode = doc.createElement(ParameterNames.RWD.toString());
			rewardNode.setTextContent(reward);
			
			ruleNode.appendChild(operatorNode);
			ruleNode.appendChild(stateNode);
			ruleNode.appendChild(degreeNode);
			ruleNode.appendChild(rewardNode);
			
			rules.appendChild(ruleNode);
		}
		
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer;
		try {
			transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(this.filename));
			transformer.transform(source, result);
		} catch (TransformerException e) {
			logger.severe("Impossible to persist the rule because " + e);
		}
	}
	
	public void initParameters() {
		Document doc = this.getDocument();
		final Element rules = (Element) doc.getElementsByTagName(ParameterNames.RULES.toString()).item(0);
		final NodeList ruleList = rules.getElementsByTagName(ParameterNames.RULE.toString());
		int nbRules = ruleList.getLength();
		for(int i = 0; i < nbRules; i++){
			String op = "";
			Double lb = 0.0;
			Double ub = 0.0;
			Integer deg = 1;
			Double rwd = 0.0;
			
			final Element rule = (Element) rules.getElementsByTagName(ParameterNames.RULE.toString()).item(i);
			final NodeList operator = rule.getElementsByTagName(ParameterNames.OP.toString());
			op = operator.item(0).getTextContent();
			final NodeList state = rule.getElementsByTagName(ParameterNames.STATE.toString());
			String[] stateToString = state.item(0).getTextContent().split(";");
			lb = Double.parseDouble(stateToString[0]);
			ub = Double.parseDouble(stateToString[1]);
			final NodeList degree = rule.getElementsByTagName(ParameterNames.DEG.toString());
			deg = Integer.parseInt(degree.item(0).getTextContent());
			final NodeList reward = rule.getElementsByTagName(ParameterNames.RWD.toString());
			rwd = Double.parseDouble(reward.item(0).getTextContent());
			KnowledgeRule krule = new KnowledgeRule(op, lb, ub, deg, rwd);
			this.rules.add(krule);
		}
	}
	
	public Integer bestDegree(Double rate, Integer currDeg, boolean underused, boolean overused){
		Integer degree = currDeg;
		for(KnowledgeRule rule : this.rules){
			if(rule.isApplicable(rate) && rule.getDegree() != degree){
				degree = rule.getDegree();
				break;
			}
		}
		if(degree == currDeg && underused){
			degree = degree - 1;
		}
		if(degree == currDeg && overused){
			degree = degree + 1;
		}
		return degree;
	}
}
