/**
 * 
 */
package storm.autoscale.scheduler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.xml.sax.SAXException;

import storm.autoscale.scheduler.action.ScaleActionTrigger;
import storm.autoscale.scheduler.config.XmlKnowledgeParser;
import storm.autoscale.scheduler.config.XmlThresholdsParser;
import storm.autoscale.scheduler.config.knowledge.KnowledgeRule;
import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.modules.assignment.AssignmentMonitor;
import storm.autoscale.scheduler.modules.component.ComponentMonitor;
import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;
import storm.autoscale.scheduler.modules.scale.ScalingManagerPlus;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;
import storm.autoscale.scheduler.util.UtilFunctions;

/**
 * @author Roland
 *
 */
public class RLearningScheduler implements IScheduler {

	@SuppressWarnings("rawtypes")
	Map conf;
	private ScalingManagerPlus scaleManager;
	private ComponentMonitor compMonitor;
	private AssignmentMonitor assignMonitor;
	private TopologyExplorer explorer;
	private String nimbusHost;
	private Integer nimbusPort;
	private XmlConfigParser parser;
	private XmlThresholdsParser thresholdParser;
	private XmlKnowledgeParser knowledgeParser;
	private Double activityMin;
	private Double activityMax;
	
	private static final Double scale = 10.0;
	private static Logger logger = Logger.getLogger("RLearningScheduler");
	
	public RLearningScheduler() {
		logger.info("The learning-based scheduler for Storm is starting...");
	}
	
	/* (non-Javadoc)
	 * @see org.apache.storm.scheduler.IScheduler#prepare(java.util.Map)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) {
		this.conf = conf;
		try {
			this.parser = new XmlConfigParser("./conf/autoscale_parameters.xml");
			this.thresholdParser = new XmlThresholdsParser("./conf/threshold_parameters.xml");
			this.knowledgeParser = new XmlKnowledgeParser("./conf/knowledge_base.xml");
			this.parser.initParameters();
			this.thresholdParser.initParameters();
			this.knowledgeParser.initParameters();
			this.nimbusHost = parser.getNimbusHost();
			this.nimbusPort = parser.getNimbusPort();
			this.activityMin = thresholdParser.getActivityMin();
			this.activityMax = thresholdParser.getActivityMax();
		} catch (ParserConfigurationException | SAXException | IOException e) {
			logger.severe("Unable to load the configuration file for AUTOSCALE because " + e);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.scheduler.IScheduler#schedule(org.apache.storm.scheduler.Topologies, org.apache.storm.scheduler.Cluster)
	 */
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		String host = this.parser.getDbHost();
		String name = this.parser.getDbName();
		String user = this.parser.getDbUser();
		String pwd = this.parser.getDbPassword();
		
		Integer monitFrequency = this.parser.getMonitoringFrequency();
		StatStorageManager manager = null;
		try {
			manager = StatStorageManager.getManager(host, name, user, pwd, this.nimbusHost, this.nimbusPort, monitFrequency);
			manager.storeStatistics();
		} catch (ClassNotFoundException | SQLException e1) {
			logger.severe("Unable to start the StatStorageManage because of " + e1);
		}
		
		for(TopologyDetails topology : topologies.getTopologies()){
			if(!manager.isActive(topology.getId())){
				logger.fine("Topology " + topology.getName() + " is inactive, killed or being rebalanced...");
			}else{
				this.compMonitor = new ComponentMonitor(this.parser, this.nimbusHost, this.nimbusPort);
				if(!manager.existConstraint(topology.getName())){
					manager.storeTopologyConstraints(this.compMonitor.getTimestamp(), topology);
				}
				this.assignMonitor = new AssignmentMonitor(cluster, topology);
				this.explorer = new TopologyExplorer(topology.getId(), topology.getTopology());
				this.assignMonitor.update();
				this.compMonitor.getStatistics(explorer);
				this.scaleManager = new ScalingManagerPlus();
				this.scaleManager.initDegrees(compMonitor, assignMonitor);
				if(!this.compMonitor.getRegisteredComponents().isEmpty()){
					ArrayList<String> bolts = this.explorer.getBolts();
					for(String bolt : bolts){
						HashMap<Integer, Long> nbInputs = this.compMonitor.getStats(bolt).getInputRecords();
						Set<Integer> timestamps = nbInputs.keySet();
						Double sum = 0.0;
						Integer count = 0;
						Boolean minFlag = false;
						Boolean maxFlag = false;
						ArrayList<Long> execRecords = new ArrayList<>();
						ArrayList<Long> inRecords = new ArrayList<>();
						Collection<Long> rawExecRecords = this.compMonitor.getStats(bolt).getExecutedRecords().values();
						Collection<Long> rawInRecords = this.compMonitor.getStats(bolt).getInputRecords().values();
						
						for(Long record : rawExecRecords){
							execRecords.add(record);
						}
						for(Long record : rawInRecords){
							inRecords.add(record);
						}
						
						Double avgProcessingRate = UtilFunctions.getAvgValue(execRecords);
						Double avgInputRate = UtilFunctions.getAvgValue(inRecords);
						Double reward = avgInputRate / avgProcessingRate;
						if(reward < this.activityMin){
							minFlag = true;
						}
						if(reward > this.activityMax){
							maxFlag = true;
						}
						
						for(Integer timestamp: timestamps){
							Double rate = (nbInputs.get(timestamp) / monitFrequency) * 1.0;
							sum += rate;
							count++;
						}
						Double averageRate = sum / count;
	
						Double lBound = ((Math.round(averageRate)) / Math.round(scale)) * scale;
						Double uBound = lBound + scale;
						Integer degree = this.assignMonitor.getParallelism(bolt);
						
					
						knowledgeParser.createOrUpdateRule(new KnowledgeRule(bolt, lBound, uBound, degree, reward));
						
						if(minFlag){
							this.scaleManager.addScaleInAction(bolt, this.knowledgeParser.bestDegree(averageRate, degree, minFlag, maxFlag));
						}
						if(maxFlag){
							this.scaleManager.addScaleOutAction(bolt, this.knowledgeParser.bestDegree(averageRate, degree, minFlag, maxFlag));
						}
					}
					@SuppressWarnings("unused")
					ScaleActionTrigger trigger = new ScaleActionTrigger(nimbusHost, nimbusPort, compMonitor, scaleManager, assignMonitor.getNbWorkers(), topology);
				}
			}
		}
		/*Then we let the resource aware scheduler distribute the load*/
		ResourceAwareScheduler scheduler = new ResourceAwareScheduler();
		scheduler.prepare(this.conf);
		scheduler.schedule(topologies, cluster);
	}

}
