/**
 * 
 */
package storm.autoscale.scheduler;

/*import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;*/
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
	private String nimbusHost;
	private Integer nimbusPort;
	private static Logger logger = Logger.getLogger("AutoscaleScheduler");
	//private FileWriter writer;

	/**
	 * 
	 */
	public AutoscaleScheduler() {
		/*try {
			Path path = Paths.get("autoscaleLogs.csv");
			if(!Files.exists(path)){
				Files.createFile(path);				
			}
			File file = path.toFile();
			this.writer = new FileWriter(file);
			this.writer.write("timestamp;component;input;variation;executed;output;latency;congested;\n");
		} catch (SecurityException | IOException e) {
			logger.severe("Unable to initialize the log file because " + e);
		}*/
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
				logger.fine("Topology " + topology.getName() + " is inactive, killed or being rebalanced...");
			}else{
				this.compMonitor = new ComponentMonitor("localhost", this.nimbusHost, this.nimbusPort, 2000);
				this.assignMonitor = new AssignmentMonitor(cluster, topology);
				this.explorer = new TopologyExplorer(topology.getName(), topology.getTopology());
				this.assignMonitor.update();
				this.compMonitor.getStatistics(explorer);
				Integer timestamp = manager.getCurrentTimestamp();
				//if(timestamp >= ComponentMonitor.WINDOW_SIZE){
					this.congested = this.compMonitor.getCongested();
					HashMap<String, ComponentWindowedStats> congestedStats = new HashMap<>();
					int oldestTimestamp = Math.max(0, timestamp - ComponentMonitor.WINDOW_SIZE);
					String monitoring = "Current monitoring info (from timestamp " + oldestTimestamp + " to timestamp " + timestamp + ")\n";
					logger.fine(monitoring);
					for(String component : this.compMonitor.getRegisteredComponents()){
						ComponentWindowedStats stats = this.compMonitor.getStats(component);
						congestedStats.put(component, stats);

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
						
						boolean isCongested = false;
						if(this.congested.contains(component)){
							isCongested = true;
						}
						/*try {
							this.writer.write(timestamp + ";" + component + ";" + globalInputVar + ";" + growth + ";" + globalExecutedVar + ";" + globalOutputVar + ";" + lastAvgLatencyRecord + ";" + isCongested + "\n");
						} catch (IOException e) {
							logger.warning("Unable to log component logs because" + e);
						} */
					}
					if(this.congested.isEmpty()){
						logger.fine("No component to scale out!");
					}else{
						String congestInfo = "Congested components: ";
						for(String component : this.congested){
							congestInfo += component + " ";
						}
						congestInfo += "have been detected!";
						logger.fine(congestInfo);
						IAction action = new ScaleOutAction(congestedStats, topology, assignMonitor, new DelegatedAllocationStrategy(assignMonitor), this.nimbusHost, this.nimbusPort);
					}
				}
			//}
			/*Then we let the default scheduler balance the load*/
			EvenScheduler scheduler = new EvenScheduler();
			scheduler.schedule(topologies, cluster);
		}
	}
}