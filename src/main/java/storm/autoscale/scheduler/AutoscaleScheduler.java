/**
 * 
 */
package storm.autoscale.scheduler;

import java.sql.SQLException;
import java.util.ArrayList;
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
import storm.autoscale.scheduler.modules.stats.ComponentStats;
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
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#schedule(backtype.storm.scheduler.Topologies, backtype.storm.scheduler.Cluster)
	 */
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		StatStorageManager manager = null;
		try {
			manager = StatStorageManager.getManager("localhost", "localhost", 6627, 1000);
		} catch (ClassNotFoundException | SQLException e1) {
			logger.severe("Unable to start the StatStorageManage because of " + e1);
		}

		/*In a first time, we take all scaling decisions*/
		for(TopologyDetails topology : topologies.getTopologies()){

			this.compMonitor = new ComponentMonitor("localhost", "localhost", 6627, 1000);
			this.assignMonitor = new AssignmentMonitor(cluster, topology);
			this.explorer = new TopologyExplorer(topology.getTopology());
			this.assignMonitor.update();
			this.compMonitor.getStatistics(explorer);
			/*IMetric impactMetric = new WelfMetric(this.explorer, this.components);

			this.congested = this.components.getCongested();
			this.toAssign = new ArrayList<>();*/

			String monitoring = "Current monitoring info (timestamp " + manager.getCurrentTimestamp() + ")\n";
			logger.info(monitoring);
			for(String component : this.compMonitor.getRegisteredComponents()){
				ComponentStats stats = this.compMonitor.getStats(component);
				String infos = "Component " + component + " ---> inputs: " + stats.getNbInputs();
				infos += ", executed: " + stats.getNbExecuted();
				infos += ", outputs: " + stats.getNbOutputs(); 
				infos += ", latency: " + stats.getAvgLatency() + "\n";
				logger.info(infos);
			}
			this.congested = this.compMonitor.getCongested();
			if(this.congested.isEmpty()){
				this.isScaled = true;
				logger.info("No component to scale out!");
			}else{
				this.isScaled = false;
				String congestInfo = "Congested compMonitor ";
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
					IAction action = new ScaleOutAction(congestedComponent, topology, cluster, compMonitor, assignMonitor, new DelegatedAllocationStrategy(assignMonitor));
					action.scale();
					this.congested.remove(congestedComponent);
					/*String mostImportantComponent = this.congested.get(0);
					for(String component : this.congested){
						if(impactMetric.compute(component) > impactMetric.compute(mostImportantComponent)){
							mostImportantComponent = component;
						}
					}
					incrParallelism(mostImportantComponent);
					this.congested.remove(mostImportantComponent);
					logger.info("Component " + mostImportantComponent + " is being uncongested...");
					forkAndAssign(mostImportantComponent, cluster, topology);*/
				}
			}

		}

		/*Then we let the default scheduler balance the load*/
		EvenScheduler scheduler = new EvenScheduler();
		scheduler.schedule(topologies, cluster);
	}
}