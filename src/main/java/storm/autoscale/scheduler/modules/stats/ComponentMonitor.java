/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

import storm.autoscale.scheduler.metrics.EPRMetric;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class ComponentMonitor {
	
	private HashMap<String, ComponentWindowedStats> stats;
	private StatStorageManager manager;
	private Integer timestamp;
	private Integer samplingRate;
	private ArrayList<String> scaleOutRequirements;
	private ArrayList<String> scaleInRequirements;
	public static final Integer WINDOW_SIZE = 10;
	public static final Double RECORD_THRESHOLD = 0.7;
	public static final Double VAR_THRESHOLD = 20.0;
	private static Logger logger = Logger.getLogger("ComponentMonitor");
	
	/**
	 * 
	 */
	public ComponentMonitor(String dbHost, String nimbusHost, Integer nimbusPort, Integer rate) {
		this.stats = new HashMap<>();
		this.samplingRate = rate; 
		if(nimbusHost != null && nimbusPort != null){
			try {
				this.manager = StatStorageManager.getManager(dbHost, nimbusHost, nimbusPort, rate);
				this.timestamp = this.manager.getCurrentTimestamp();
			} catch (SQLException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public void getStatistics(TopologyExplorer explorer){
		this.reset();
		this.timestamp = this.manager.getCurrentTimestamp(); 
		Set<String> spouts = explorer.getSpouts();
		for(String spout : spouts){
			
			HashMap<Integer, Long> outputRecords = this.manager.getSpoutOutputs(spout, this.timestamp, WINDOW_SIZE);
			HashMap<Integer, Double> avgTopLatencyRecords = this.manager.getTopologyAvgLatency(explorer.getTopologyName(), this.timestamp, WINDOW_SIZE);
		
			HashMap<Integer, Long> inputRecords = new HashMap<>();
			HashMap<Integer, Long> executedRecords = new HashMap<>();
			HashMap<Integer, Double> selectivityRecords = new HashMap<>();
			ArrayList<Integer> recordedTimestamps = ComponentWindowedStats.getRecordedTimestamps(outputRecords);
			for(Integer timestamp : recordedTimestamps){
				inputRecords.put(timestamp, 0L);
				executedRecords.put(timestamp, 0L);
				selectivityRecords.put(timestamp, 1.0);
			}
			
			ComponentWindowedStats componentRecords = new ComponentWindowedStats(spout, inputRecords, executedRecords, outputRecords, avgTopLatencyRecords, selectivityRecords);
			this.stats.put(spout, componentRecords);
		}
		Set<String> bolts = explorer.getBolts();
		for(String bolt : bolts){
			HashMap<Integer, Long> inputRecords = new HashMap<>();
			ArrayList<String> parents = explorer.getParents(bolt);
			for(String parent : parents){
				HashMap<Integer, Long> parentOutputRecords = new HashMap<>();
				if(this.stats.containsKey(parent)){
					parentOutputRecords = this.stats.get(parent).getOutputRecords();
				}else{
					if(explorer.getSpouts().contains(parent)){
						parentOutputRecords = this.manager.getSpoutOutputs(parent, this.timestamp, WINDOW_SIZE);
					}
					if(explorer.getBolts().contains(parent)){
						parentOutputRecords = this.manager.getBoltOutputs(parent, this.timestamp, WINDOW_SIZE);
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
			HashMap<Integer, Long> executedRecords = this.manager.getExecuted(bolt, this.timestamp, WINDOW_SIZE);
			HashMap<Integer, Long> outputRecords = this.manager.getBoltOutputs(bolt, this.timestamp, WINDOW_SIZE);
			HashMap<Integer, Double> avgLatencyRecords = this.manager.getAvgLatency(bolt, this.timestamp, WINDOW_SIZE);
			HashMap<Integer, Double> selectivityRecords = this.manager.getSelectivity(bolt, this.timestamp, WINDOW_SIZE);
			ComponentWindowedStats component = new ComponentWindowedStats(bolt, inputRecords, executedRecords, outputRecords, avgLatencyRecords, selectivityRecords);
			this.stats.put(bolt, component);
		}
	}
	
	public Set<String> getRegisteredComponents(){
		return this.stats.keySet();
	}
	
	public ComponentWindowedStats getStats(String component){
		return this.stats.get(component);
	}
	
	public void updateStats(String component, ComponentWindowedStats cws){
		if(this.stats.containsKey(component)){
			this.stats.remove(component);
		}
		this.stats.put(component, cws);
	}
	
	public Integer getSamplingRate(){
		return this.samplingRate;
	}
	
	public boolean isInputDecreasing(String component){
		boolean result = false;
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		ArrayList<Double> variations = ComponentWindowedStats.getVariations(inputRecords);
		int count = 0;
		int nbVectors = variations.size();
		double threshold = nbVectors * RECORD_THRESHOLD;
		for(int i = 0; i < nbVectors; i++){
			if(variations.get(i) < -VAR_THRESHOLD){
				count++;
			}
		}
		if(count >= threshold){
			result = true;
		}
		return result;
	}
	
	public boolean isInputStable(String component){
		boolean result = false;
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		ArrayList<Double> variations = ComponentWindowedStats.getVariations(inputRecords);
		int count = 0;
		int nbVectors = variations.size();
		double threshold = nbVectors * RECORD_THRESHOLD;
		for(int i = 0; i < nbVectors; i++){
			if(variations.get(i) > -VAR_THRESHOLD && variations.get(i) < VAR_THRESHOLD){
				count++;
			}
		}
		if(count >= threshold){
			result = true;
		}
		return result;
	}
	
	public boolean isInputIncreasing(String component){
		boolean result = false;
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		ArrayList<Double> variations = ComponentWindowedStats.getVariations(inputRecords);
		int count = 0;
		int nbVectors = variations.size();
		double threshold = nbVectors * RECORD_THRESHOLD;
		for(int i = 0; i < nbVectors; i++){
			if(variations.get(i) > VAR_THRESHOLD){
				count++;
			}
		}
		if(count >= threshold){
			result = true;
		}
		return result;
	}
	
	public HashMap<String, Long> getFormerRemainingTuples(){
		HashMap<String, Long> result = new HashMap<>();
		for(String component : this.stats.keySet()){
			result.put(component, this.manager.getFormerRemainingTuples(component));
		}
		return result;
	}
	
	public void trackRequirements(TopologyExplorer explorer){
		EPRMetric metric = new EPRMetric(explorer, this);
		for(String component : this.stats.keySet()){
			//TODO
			//store the info
			//compute the epr
			//apply rules defined in the proposition
			//fulfill scale out and scale in lists
		}
	}
	
	public ArrayList<String> getScaleOutDecisions(){
		return this.scaleOutRequirements;
	}
	
	public ArrayList<String> getScaleInDecisions(){
		return this.scaleInRequirements;
	}
	
	public void reset(){
		this.stats = new HashMap<>();
	}
}