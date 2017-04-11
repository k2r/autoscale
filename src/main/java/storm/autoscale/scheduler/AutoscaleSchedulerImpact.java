/**
 * 
 */
package storm.autoscale.scheduler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.xml.sax.SAXException;

import storm.autoscale.scheduler.action.IAction;
import storm.autoscale.scheduler.action.ScaleInAction;
import storm.autoscale.scheduler.action.ScaleOutAction;
import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.metrics.ActivityMetric;
import storm.autoscale.scheduler.metrics.IMetric;
import storm.autoscale.scheduler.metrics.ImpactMetric;
import storm.autoscale.scheduler.modules.assignment.AssignmentMonitor;
import storm.autoscale.scheduler.modules.component.ComponentMonitor;
import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;
import storm.autoscale.scheduler.modules.scale.ScalingManager;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;

/**
 * @author Roland
 *
 */
public class AutoscaleSchedulerImpact implements IScheduler {
	
	@SuppressWarnings("rawtypes")
	Map conf;
	private ScalingManager scaleManager;
	private ComponentMonitor compMonitor;
	private AssignmentMonitor assignMonitor;
	private TopologyExplorer explorer;
	private String nimbusHost;
	private Integer nimbusPort;
	private XmlConfigParser parser;
	
	private static Logger logger = Logger.getLogger("AutoscaleSchedulerImpact");
	
	public AutoscaleSchedulerImpact() {
		logger.info("The auto-scaling scheduler for Storm is starting...");
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
			this.parser.initParameters();
			this.nimbusHost = parser.getNimbusHost();
			this.nimbusPort = parser.getNimbusPort();
		} catch (ParserConfigurationException | SAXException | IOException e) {
			logger.severe("Unable to load the configuration file for AUTOSCALE because " + e);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.scheduler.IScheduler#schedule(org.apache.storm.scheduler.Topologies, org.apache.storm.scheduler.Cluster)
	 */
	@SuppressWarnings("unused")
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

		/*In a first time, we take all scaling decisions*/
		for(TopologyDetails topology : topologies.getTopologies()){
			if(!manager.isActive(topology.getId())){
				logger.fine("Topology " + topology.getName() + " is inactive, killed or being rebalanced...");
			}else{
				this.compMonitor = new ComponentMonitor(this.parser, this.nimbusHost, this.nimbusPort);
				if(!manager.existConstraint(topology.getName())){
					manager.storeTopologyConstraints(this.compMonitor.getTimestamp(), topology);
				}
				this.assignMonitor = new AssignmentMonitor(cluster, topology);
				this.explorer = new TopologyExplorer(topology.getName(), topology.getTopology());
				this.assignMonitor.update();
				this.compMonitor.getStatistics(explorer);
				if(!this.compMonitor.getRegisteredComponents().isEmpty()){
					this.scaleManager = new ScalingManager(compMonitor, parser);
					this.scaleManager.buildDegreeMap(assignMonitor);
					IMetric activityMetric = new ActivityMetric(this.scaleManager, this.explorer);
					this.scaleManager.buildActionGraph(activityMetric, assignMonitor);
					IMetric impactMetric = new ImpactMetric(this.scaleManager, this.explorer);
					this.scaleManager.autoscaleAlgorithmWithImpact(impactMetric, this.explorer.getAncestors(), this.explorer, this.assignMonitor);

					if(this.scaleManager.getScaleOutActions().isEmpty()){
						logger.fine("No component to scale out!");
					}else{
						String scaleOutInfo = "Components requiring scale out: ";
						for(String component : this.scaleManager.getScaleOutActions().keySet()){
							scaleOutInfo += component + " ";
						}
						scaleOutInfo += "have required a scale out!";
						logger.fine(scaleOutInfo);
						IAction action = new ScaleOutAction(this.scaleManager, this.explorer, this.assignMonitor, this.nimbusHost, this.nimbusPort);
					}

					if(this.scaleManager.getScaleInActions().isEmpty()){
						logger.fine("No component to scale in!");
					}else{
						String scaleInInfo = "Components requiring scale in: ";
						for(String component : this.scaleManager.getScaleInActions().keySet()){
							scaleInInfo += component + " ";
						}
						scaleInInfo += "have required a scale in!";
						logger.fine(scaleInInfo);
						IAction action = new ScaleInAction(this.scaleManager, this.explorer, this.assignMonitor, this.nimbusHost, this.nimbusPort);
					}
				}
			}			
		}
		/*Then we let the resource aware scheduler distribute the load*/
		ResourceAwareScheduler scheduler = new ResourceAwareScheduler();
		scheduler.prepare(this.conf);
		scheduler.schedule(topologies, cluster);
	}
}
