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
import storm.autoscale.scheduler.actions.ScaleInAction;
import storm.autoscale.scheduler.actions.ScaleOutAction;
import storm.autoscale.scheduler.allocation.DelegatedAllocationStrategy;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
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
	private ArrayList<String> needScaleOut;
	private ArrayList<String> needScaleIn;
	private String nimbusHost;
	private Integer nimbusPort;
	private String password;
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
		this.password = null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#schedule(backtype.storm.scheduler.Topologies, backtype.storm.scheduler.Cluster)
	 */
	@SuppressWarnings("unused")
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		StatStorageManager manager = null;
		try {
			manager = StatStorageManager.getManager("localhost", this.password, this.nimbusHost, this.nimbusPort, 10);
		} catch (ClassNotFoundException | SQLException e1) {
			logger.severe("Unable to start the StatStorageManage because of " + e1);
		}

		/*In a first time, we take all scaling decisions*/
		for(TopologyDetails topology : topologies.getTopologies()){
			if(!manager.isActive(topology.getId())){
				logger.fine("Topology " + topology.getName() + " is inactive, killed or being rebalanced...");
			}else{
				this.compMonitor = new ComponentMonitor("localhost", this.password, this.nimbusHost, this.nimbusPort, 10);
				this.assignMonitor = new AssignmentMonitor(cluster, topology);
				this.explorer = new TopologyExplorer(topology.getName(), topology.getTopology());
				this.assignMonitor.update();
				this.compMonitor.getStatistics(explorer);
				Integer timestamp = manager.getCurrentTimestamp();
				if(!this.compMonitor.getRegisteredComponents().isEmpty()){
					this.compMonitor.trackRequirements(explorer);
					this.compMonitor.validateRequirements(explorer.getSpouts(), explorer);
					this.needScaleOut = this.compMonitor.getScaleOutRequirements();
					this.needScaleIn = this.compMonitor.getScaleInRequirements();
					/*int oldestTimestamp = Math.max(0, timestamp - ComponentMonitor.WINDOW_SIZE);
					String monitoring = "Current monitoring info (from timestamp " + oldestTimestamp + " to timestamp " + timestamp + ")\n";
					logger.fine(monitoring);
					for(String component : this.compMonitor.getRegisteredComponents()){
						ComponentWindowedStats stats = this.compMonitor.getStats(component);

						boolean decreasing = this.compMonitor.isInputDecreasing(component);
						boolean stable = this.compMonitor.isInputStable(component);
						boolean increasing = this.compMonitor.isInputIncreasing(component);
						String growth = "undefined";
						if(decreasing){
							growth = "decreasing";
						}else{
							if(stable){
								growth = "stable";
							}else{
								if(increasing){
									growth = "increasing";
								}
							}
						}
						Double lastInputRecord = ComponentWindowedStats.getLastRecord(stats.getInputRecords());
						Double oldestInputRecord = ComponentWindowedStats.getOldestRecord(stats.getInputRecords());
						Double globalInputVar = lastInputRecord - oldestInputRecord;
						Double lastOutputRecord = ComponentWindowedStats.getLastRecord(stats.getOutputRecords());
						Double oldestOutputRecord = ComponentWindowedStats.getOldestRecord(stats.getOutputRecords());
						Double globalOutputVar = lastOutputRecord - oldestOutputRecord;
						Double lastExecutedRecord = ComponentWindowedStats.getLastRecord(stats.getExecutedRecords());
						Double oldestExecutedRecord = ComponentWindowedStats.getOldestRecord(stats.getExecutedRecords());
						Double globalExecutedVar = lastExecutedRecord - oldestExecutedRecord;
						Double lastAvgLatencyRecord = ComponentWindowedStats.getLastRecord(stats.getAvgLatencyRecords());
						Double lastSelectivityRecord = ComponentWindowedStats.getLastRecord(stats.getSelectivityRecords());

						String infos = "Component " + component + " : \n";
						infos += "\t input : " + lastInputRecord + " (" + growth + "), variation on window: " + globalInputVar + " tuple(s) \n";
						infos += "\t executed : " + lastExecutedRecord + ", variation on window: " + globalExecutedVar + " tuple(s) \n";
						infos += "\t output : " + lastOutputRecord + ", variation on window: " + globalOutputVar + " tuple(s) \n";
						infos += "\t latency : " + lastAvgLatencyRecord + " milliseconds per tuple \n";
						infos += "\t selectivity : " + lastSelectivityRecord + "\n";
						logger.fine(infos);
					}*/
					if(this.needScaleOut.isEmpty()){
						logger.fine("No component to scale out!");
					}else{
						String scaleOutInfo = "Components requiring scale out: ";
						for(String component : this.needScaleOut){
							scaleOutInfo += component + " ";
						}
						scaleOutInfo += "have required a scale out!";
						logger.fine(scaleOutInfo);
						IAction action = new ScaleOutAction(this.compMonitor, this.explorer, this.assignMonitor, new DelegatedAllocationStrategy(assignMonitor), this.nimbusHost, this.nimbusPort, this.password);
					}

					if(this.needScaleIn.isEmpty()){
						logger.fine("No component to scale in!");
					}else{
						String scaleInInfo = "Components requiring scale in: ";
						for(String component : this.needScaleIn){
							scaleInInfo += component + " ";
						}
						scaleInInfo += "have required a scale in!";
						logger.fine(scaleInInfo);
						IAction action = new ScaleInAction(this.compMonitor, this.explorer, this.assignMonitor, new DelegatedAllocationStrategy(assignMonitor), this.nimbusHost, this.nimbusPort, this.password);
					}
				}
			}
			/*Then we let the default scheduler balance the load*/
			EvenScheduler scheduler = new EvenScheduler();
			scheduler.schedule(topologies, cluster);
		}
	}
}