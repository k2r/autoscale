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
//import java.util.ArrayList;
import java.util.HashMap;
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
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;

/**
 * @author Roland
 *
 */
public class MonitoredEvenScheduler implements IScheduler{

	private ComponentMonitor compMonitor;
	private AssignmentMonitor assignMonitor;
	private TopologyExplorer explorer;
	//private ArrayList<String> congested;
	private String nimbusHost;
	private Integer nimbusPort;
	private static Logger logger = Logger.getLogger("MonitoredEvenScheduler");
	//private FileWriter writer;
	/**
	 * 
	 */
	public MonitoredEvenScheduler() {
		/*try {
			Path path = Paths.get("evenLogs.csv");
			if(!Files.exists(path)){
				Files.createFile(path);				
			}
			File file = path.toFile();
			this.writer = new FileWriter(file);
			this.writer.write("timestamp;component;input;variation;executed;output;latency;congested;\n");
			} catch (SecurityException | IOException e) {
			logger.severe("Unable to initialize the log file because " + e);
		}*/
		logger.info("Monitoring the storm even scheduler...");
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) {
		this.nimbusHost = (String) conf.get("nimbus.host");
		this.nimbusPort = (Integer) conf.get("nimbus.thrift.port");	
	}
	
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		StatStorageManager manager = null;
		try {
			manager = StatStorageManager.getManager("localhost", this.nimbusHost, this.nimbusPort, 2000);
		} catch (ClassNotFoundException | SQLException e1) {
			logger.severe("Unable to start the StatStorageManage because of " + e1);
		}
		for(TopologyDetails topology : topologies.getTopologies()){
			if(!manager.isActive(topology.getId())){
				logger.info("Topology " + topology.getName() + " is being rebalanced...");
			}else{
				this.compMonitor = new ComponentMonitor("localhost", this.nimbusHost, this.nimbusPort, 2000);
				this.assignMonitor = new AssignmentMonitor(cluster, topology);
				this.explorer = new TopologyExplorer(topology.getName(), topology.getTopology());
				this.assignMonitor.update();
				this.compMonitor.getStatistics(explorer);
				Integer timestamp = manager.getCurrentTimestamp();
				//if(timestamp >= ComponentMonitor.WINDOW_SIZE){
					//this.congested = this.compMonitor.getCongested();
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
						
						/*boolean isCongested = false;
						if(this.congested.contains(component)){
							isCongested = true;
						}
						try {
							this.writer.write(timestamp + ";" + component + ";" + globalInputVar + ";" + growth + ";" + globalExecutedVar + ";" + globalOutputVar + ";" + lastAvgLatencyRecord + ";" + isCongested + "\n");
						} catch (IOException e) {
							logger.warning("Unable to log component logs because" + e);
						}*/ 
					}
				//}
			}
		}
		EvenScheduler scheduler = new EvenScheduler();
		scheduler.schedule(topologies, cluster);
	}
}