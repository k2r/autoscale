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

import storm.autoscale.scheduler.actions.IAction;
import storm.autoscale.scheduler.actions.ScaleInAction;
import storm.autoscale.scheduler.actions.ScaleOutAction;
import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.metrics.ActivityMetric;
import storm.autoscale.scheduler.metrics.IMetric;
import storm.autoscale.scheduler.metrics.ImpactMetric;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.StatStorageManager;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class AutoscaleSchedulerImpact implements IScheduler {

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
		this.nimbusHost = (String) conf.get("nimbus.host");
		this.nimbusPort = (Integer) conf.get("nimbus.thrift.port");
		try {
			this.parser = new XmlConfigParser("conf/autoscale_parameters.xml");
			this.parser.initParameters();
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
				this.assignMonitor = new AssignmentMonitor(cluster, topology);
				this.explorer = new TopologyExplorer(topology.getName(), topology.getTopology());
				this.assignMonitor.update();
				this.compMonitor.getStatistics(explorer);
				Integer timestamp = manager.getCurrentTimestamp();
				if(!this.compMonitor.getRegisteredComponents().isEmpty()){
					this.compMonitor.buildDegreeMap(assignMonitor);
					IMetric activityMetric = new ActivityMetric(this.compMonitor, this.explorer);
					this.compMonitor.buildActionGraph(activityMetric, assignMonitor);
					IMetric impactMetric = new ImpactMetric(this.compMonitor, this.explorer);
					this.compMonitor.autoscaleAlgorithmWithImpact(impactMetric, this.explorer.getAncestors(), this.explorer, this.assignMonitor);

					if(this.compMonitor.getScaleOutActions().isEmpty()){
						logger.fine("No component to scale out!");
					}else{
						String scaleOutInfo = "Components requiring scale out: ";
						for(String component : this.compMonitor.getScaleOutActions().keySet()){
							scaleOutInfo += component + " ";
						}
						scaleOutInfo += "have required a scale out!";
						logger.fine(scaleOutInfo);
						IAction action = new ScaleOutAction(this.compMonitor, this.explorer, this.assignMonitor, this.nimbusHost, this.nimbusPort);
					}

					if(this.compMonitor.getScaleInActions().isEmpty()){
						logger.fine("No component to scale in!");
					}else{
						String scaleInInfo = "Components requiring scale in: ";
						for(String component : this.compMonitor.getScaleInActions().keySet()){
							scaleInInfo += component + " ";
						}
						scaleInInfo += "have required a scale in!";
						logger.fine(scaleInInfo);
						IAction action = new ScaleInAction(this.compMonitor, this.explorer, this.assignMonitor, this.nimbusHost, this.nimbusPort);
					}
				}
			}
			/*Then we let the default scheduler balance the load*/
			ResourceAwareScheduler scheduler = new ResourceAwareScheduler();
			scheduler.schedule(topologies, cluster);
		}
	}

}
