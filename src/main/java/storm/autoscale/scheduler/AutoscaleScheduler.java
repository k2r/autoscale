/**
 * 
 */
package storm.autoscale.scheduler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import storm.autoscale.scheduler.actions.IAction;
import storm.autoscale.scheduler.actions.ScaleOutAction;
import storm.autoscale.scheduler.allocation.DelegatedAllocationStrategy;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */

public class AutoscaleScheduler implements IScheduler {
	
	private ComponentMonitor compMonitor;
	private AssignmentMonitor assignMonitor;
	private TopologyExplorer explorer;
	private ArrayList<String> congested;
	private boolean isScaled;
	private String nimbusHost;
	private Integer nimbusPort;
	private static Logger logger = Logger.getLogger("AutoscaleScheduler");

	/**
	 * 
	 */
	public AutoscaleScheduler() {
		
		logger.info("The auto-scaling scheduler for Storm is starting...");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#prepare(java.util.Map)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) {
		this.nimbusHost = (String) conf.get("nimbus.host");
		this.nimbusPort = (Integer) conf.get("nimbus.thrift.port");
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#schedule(backtype.storm.scheduler.Topologies, backtype.storm.scheduler.Cluster)
	 */
	@SuppressWarnings("unused")
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		StatStorageManager manager = null;
		try {
			manager = StatStorageManager.getManager("localhost", this.nimbusHost, this.nimbusPort, 2000);
		} catch (ClassNotFoundException | SQLException e1) {
			logger.severe("Unable to start the StatStorageManage because of " + e1);
		}

		/*In a first time, we take all scaling decisions*/
		for(TopologyDetails topology : topologies.getTopologies()){
			if(!manager.isActive(topology.getId())){
				logger.info("Topology " + topology.getName() + " is being rebalanced...");
			}else{
				this.compMonitor = new ComponentMonitor("localhost", this.nimbusHost, this.nimbusPort, 2000);
				this.assignMonitor = new AssignmentMonitor(cluster, topology);
				this.explorer = new TopologyExplorer(topology.getName(), topology.getTopology());
				this.assignMonitor.update();
				this.compMonitor.getStatistics(explorer);
				this.congested = this.compMonitor.getCongested();
				HashMap<String, ComponentWindowedStats> congestedStats = new HashMap<>();
				String monitoring = "Current monitoring info (timestamp " + manager.getCurrentTimestamp() + ")\n";
				logger.info(monitoring);
				for(String component : this.compMonitor.getRegisteredComponents()){
					ComponentWindowedStats stats = this.compMonitor.getStats(component);
					congestedStats.put(component, stats);
					String infos = "Component " + component + " ---> inputs: " + stats.getNbInputs();
					infos += ", executed: " + stats.getNbExecuted();
					infos += ", outputs: " + stats.getNbOutputs(); 
					infos += ", latency: " + stats.getAvgLatency() + "\n";
					logger.info(infos);
				}
				if(this.congested.isEmpty()){
					this.isScaled = true;
					logger.info("No component to scale out!");
				}else{
					this.isScaled = false;
					String congestInfo = "Congested components: ";
					for(String component : this.congested){
						congestInfo += component + " ";
						
					}
					congestInfo += "have been detected!";
					logger.info(congestInfo);
				}
				while(!isScaled){
					if(this.congested.isEmpty()){
						this.isScaled = true;
						break;
					}else{
						String congestedComponent = this.congested.get(0);
						IAction action = new ScaleOutAction(congestedStats.get(congestedComponent), topology, assignMonitor, new DelegatedAllocationStrategy(assignMonitor));
						this.congested.remove(congestedComponent);
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}

			}

			/*Then we let the default scheduler balance the load*/
			EvenScheduler scheduler = new EvenScheduler();
			scheduler.schedule(topologies, cluster);
		}
	}
}