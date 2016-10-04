/**
 * 
 */
package storm.autoscale.scheduler;

import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;

/**
 * @author Roland
 *
 */
public class MonitoredEvenScheduler implements IScheduler{

	private ComponentMonitor compMonitor;
	private AssignmentMonitor assignMonitor;
	private TopologyExplorer explorer;
	private String password;
	private String nimbusHost;
	private Integer nimbusPort;
	private static Logger logger = Logger.getLogger("MonitoredEvenScheduler");
	
	/**
	 * 
	 */
	public MonitoredEvenScheduler() {
		logger.info("Monitoring the storm even scheduler...");
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) {
		this.nimbusHost = (String) conf.get("nimbus.host");
		this.nimbusPort = (Integer) conf.get("nimbus.thrift.port");
		this.password = null;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		StatStorageManager manager = null;
		try {
			manager = StatStorageManager.getManager("localhost", this.password, this.nimbusHost, this.nimbusPort, 10);
			manager.storeStatistics();
		} catch (ClassNotFoundException | SQLException e1) {
			logger.severe("Unable to start the StatStorageManage because of " + e1);
		}
		for(TopologyDetails topology : topologies.getTopologies()){
			if(!manager.isActive(topology.getId())){
				logger.fine("Topology " + topology.getName() + " is inactive, killed or being rebalanced...");
			}else{
				this.compMonitor = new ComponentMonitor("localhost", this.password, this.nimbusHost, this.nimbusPort, 10);
				this.assignMonitor = new AssignmentMonitor(cluster, topology);
				this.explorer = new TopologyExplorer(topology.getName(), topology.getTopology());
				this.assignMonitor.update();
				this.compMonitor.getStatistics(explorer);
				if(!this.compMonitor.getRegisteredComponents().isEmpty()){
					this.compMonitor.buildActionGraph(explorer, assignMonitor);
					this.compMonitor.autoscaleAlgorithm(explorer.getSpouts(), explorer);
			
				}
			}
		}
		EvenScheduler scheduler = new EvenScheduler();
		scheduler.schedule(topologies, cluster);
	}
}