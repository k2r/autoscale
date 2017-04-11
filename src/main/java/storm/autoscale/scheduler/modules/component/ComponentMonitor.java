/**
 * 
 */
package storm.autoscale.scheduler.modules.component;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;

/**
 * @author Roland
 *
 */
public class ComponentMonitor {
	
	
	private StatStorageManager manager;
	public XmlConfigParser parser;
	
	private Integer timestamp;
	private Integer monitFrequency;
	
	private HashMap<String, ComponentWindowedStats> stats;
	
	private static Logger logger = Logger.getLogger("ComponentMonitor");
	
	/**
	 * 
	 */
	public ComponentMonitor(XmlConfigParser parser, String nimbusHost, Integer nimbusPort) {
		this.parser = parser;
		this.monitFrequency = this.parser.getMonitoringFrequency(); 
		if(nimbusHost != null && nimbusPort != null){
			try {
				this.manager = StatStorageManager.getManager(this.parser.getDbHost(), this.parser.getDbName(), 
						this.parser.getDbUser(), this.parser.getDbPassword(), 
						nimbusHost, nimbusPort, this.parser.getMonitoringFrequency());
				this.timestamp = this.manager.getCurrentTimestamp();
			} catch (SQLException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		this.stats = new HashMap<>();
	}

	/**
	 * @return the manager
	 */
	public StatStorageManager getManager() {
		return manager;
	}

	public void getStatistics(TopologyExplorer explorer){
		this.reset();
		this.timestamp = this.manager.getCurrentTimestamp(); 
		ArrayList<String> spouts = explorer.getSpouts();
		Integer windowSize = this.parser.getWindowSize();
		for(String spout : spouts){
			
			HashMap<Integer, Long> outputRecords = this.manager.getSpoutOutputs(spout, this.timestamp, windowSize);
			HashMap<Integer, Double> avgTopLatencyRecords = this.manager.getTopologyAvgLatency(explorer.getTopologyName(), this.timestamp, windowSize);
		
			HashMap<Integer, Long> inputRecords = new HashMap<>();
			HashMap<Integer, Long> executedRecords = new HashMap<>();
			HashMap<Integer, Double> selectivityRecords = new HashMap<>();
			HashMap<Integer, ArrayList<Double>> cpuUsageRecords = new HashMap<>();
			ArrayList<Integer> recordedTimestamps = ComponentWindowedStats.getRecordedTimestamps(outputRecords);
			for(Integer timestamp : recordedTimestamps){
				inputRecords.put(timestamp, 0L);
				executedRecords.put(timestamp, 0L);
				selectivityRecords.put(timestamp, 1.0);
				cpuUsageRecords.put(timestamp, new ArrayList<Double>());
			}
			
			ComponentWindowedStats componentRecords = new ComponentWindowedStats(spout, inputRecords, executedRecords, outputRecords, avgTopLatencyRecords, selectivityRecords, cpuUsageRecords);
			this.stats.put(spout, componentRecords);
		}
		ArrayList<String> bolts = explorer.getBolts();
		for(String bolt : bolts){
			HashMap<Integer, Long> inputRecords = new HashMap<>();
			ArrayList<String> parents = explorer.getParents(bolt);
			for(String parent : parents){
				HashMap<Integer, Long> parentOutputRecords = new HashMap<>();
				if(this.stats.containsKey(parent)){
					parentOutputRecords = this.stats.get(parent).getOutputRecords();
				}else{
					if(explorer.getSpouts().contains(parent)){
						parentOutputRecords = this.manager.getSpoutOutputs(parent, this.timestamp, windowSize);
					}
					if(explorer.getBolts().contains(parent)){
						parentOutputRecords = this.manager.getBoltOutputs(parent, this.timestamp, windowSize);
					}else{
						logger.warning("The parent component " + parent + " should not belong to the topology!");
					}
				}
				ArrayList<Integer> recordedTimestamps = ComponentWindowedStats.getRecordedTimestamps(parentOutputRecords);
				for(Integer timestamp : recordedTimestamps){
					Long inputs = 0L;
					if(inputRecords.containsKey(timestamp)){
						inputs += inputRecords.get(timestamp);
						inputRecords.remove(timestamp);
					}
					inputs += parentOutputRecords.get(timestamp);
					inputRecords.put(timestamp, inputs);
				}
			}
			HashMap<Integer, Long> executedRecords = this.manager.getExecuted(bolt, this.timestamp, windowSize);
			HashMap<Integer, Long> outputRecords = this.manager.getBoltOutputs(bolt, this.timestamp, windowSize);
			HashMap<Integer, Double> avgLatencyRecords = this.manager.getAvgLatency(bolt, this.timestamp, windowSize);
			HashMap<Integer, Double> selectivityRecords = this.manager.getSelectivity(bolt, this.timestamp, windowSize);
			HashMap<Integer, ArrayList<Double>> cpuUsageRecords = this.manager.getCpuUsage(bolt, this.timestamp, windowSize);
			ComponentWindowedStats component = new ComponentWindowedStats(bolt, inputRecords, executedRecords, outputRecords, avgLatencyRecords, selectivityRecords, cpuUsageRecords);
			this.stats.put(bolt, component);
		}
	}
	
	public Set<String> getRegisteredComponents(){
		return this.stats.keySet();
	}
	
	public ComponentWindowedStats getStats(String component){
		return this.stats.get(component);
	}
	
	public boolean hasRecords(String component){
		ComponentWindowedStats stats = this.getStats(component);
		return stats.hasRecords();
	}
	
	public void updateStats(String component, ComponentWindowedStats cws){
		if(this.stats.containsKey(component)){
			this.stats.remove(component);
		}
		this.stats.put(component, cws);
	}
	
	/**
	 * @return the timestamp
	 */
	public Integer getTimestamp() {
		return timestamp;
	}

	
	
	public Integer getMonitoringFrequency(){
		return this.monitFrequency;
	}
	
	/**
	 * @return the parser
	 */
	public XmlConfigParser getParser() {
		return parser;
	}

	/**
	 * @param manager the manager to set
	 */
	public void setManager(StatStorageManager manager) {
		this.manager = manager;
	}

	/**
	 * @param parser the parser to set
	 */
	public void setParser(XmlConfigParser parser) {
		this.parser = parser;
	}
	
	public HashMap<String, Long> getPendingTuples(TopologyExplorer explorer){
		HashMap<String, Long> result = new HashMap<>();
		for(String component : this.stats.keySet()){
			ArrayList<String> parents = explorer.getParents(component);
			Long input = 0L;
			for(String parent : parents){
				String table = StatStorageManager.TABLE_BOLT;
				if(explorer.getSpouts().contains(parent)){
					table = StatStorageManager.TABLE_SPOUT;
				}
				input += this.manager.getCurrentTotalOutput(this.timestamp, parent, table);
			}
			Long executed = this.manager.getCurrentTotalExecuted(this.timestamp, component, StatStorageManager.TABLE_BOLT);
			result.put(component, Math.max(0, input - executed));
		}
		return result;
	}
	
	public void reset(){
		this.stats = new HashMap<>();
	}
	
	public HashMap<String, Double> getInitialCpuConstraints(TopologyExplorer explorer){
		HashMap<String, Double> result = new HashMap<>();
		Set<String> components = this.getRegisteredComponents();
		for(String component : components){
			Double constraint = this.manager.getInitialCpuConstraint(explorer.getTopologyName(), component);
			result.put(component, constraint);
		}
		return result;
	}
	
	public HashMap<String, Double> getCurrentCpuConstraints(TopologyExplorer explorer){
		HashMap<String, Double> result = new HashMap<>();
		Set<String> components = this.getRegisteredComponents();
		for(String component : components){
			Double constraint = this.manager.getCurrentCpuConstraint(explorer.getTopologyName(), component);
			result.put(component, constraint);
		}
		return result;
	}
	
	public HashMap<Integer, Double> getCpuUsageOnWorker(String component, String host, Integer port){
		return this.manager.getCpuUsagePerWorker(component, host, port, getTimestamp(), this.parser.getWindowSize());
	}
}