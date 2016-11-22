/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

//import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
//import java.util.logging.FileHandler;
import java.util.logging.Logger;
//import java.util.logging.SimpleFormatter;

import storm.autoscale.scheduler.metrics.ActivityMetric;
import storm.autoscale.scheduler.metrics.LoadMetric;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.util.RegressionTools;

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
	private HashMap<String, Double> activityValues;
	private HashMap<String, Double> loadValues;
	public static final Integer WINDOW_SIZE = 60;
	public static final Integer STABIL_COEFF = 1;
	public static final Double VAR_THRESHOLD = 0.2;
	public static final Double LOW_ACTIVITY_THRESHOLD = 0.4;
	public static final Double HIGH_ACTIVITY_THRESHOLD = 0.8;
	private static Logger logger = Logger.getLogger("ComponentMonitor");
	//private static FileHandler fh;
	
	/**
	 * 
	 */
	public ComponentMonitor(String dbHost, String password, String nimbusHost, Integer nimbusPort, Integer rate) {
		this.stats = new HashMap<>();
		this.scaleInRequirements = new ArrayList<>();
		this.scaleOutRequirements = new ArrayList<>();
		this.activityValues = new HashMap<>();
		this.loadValues = new HashMap<>();
		this.samplingRate = rate; 
		if(nimbusHost != null && nimbusPort != null){
			try {
				this.manager = StatStorageManager.getManager(dbHost, password, nimbusHost, nimbusPort, rate);
				this.timestamp = this.manager.getCurrentTimestamp();
			} catch (SQLException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		// This block configure the logger with handler and formatter  
        /*try {
			fh = new FileHandler("scaling.log");
			logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);
	        logger.setUseParentHandlers(false);
		} catch (SecurityException | IOException e) {
			logger.severe("Unable to create the log file for scaling decisions because " + e);
		}*/ 
	}

	public void getStatistics(TopologyExplorer explorer){
		this.reset();
		this.timestamp = this.manager.getCurrentTimestamp(); 
		ArrayList<String> spouts = explorer.getSpouts();
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

	/**
	 * @return the scaleOutRequirements
	 */
	public ArrayList<String> getScaleOutRequirements() {
		return scaleOutRequirements;
	}

	/**
	 * @param scaleOutRequirements the scaleOutRequirements to set
	 */
	public void setScaleOutRequirements(ArrayList<String> scaleOutRequirements) {
		this.scaleOutRequirements = scaleOutRequirements;
	}

	/**
	 * @return the scaleInRequirements
	 */
	public ArrayList<String> getScaleInRequirements() {
		return scaleInRequirements;
	}

	/**
	 * @param scaleInRequirements the scaleInRequirements to set
	 */
	public void setScaleInRequirements(ArrayList<String> scaleInRequirements) {
		this.scaleInRequirements = scaleInRequirements;
	}

	public boolean needScaleOut(String component){
		return this.scaleOutRequirements.contains(component);
	}
	
	public boolean needScaleIn(String component){
		return this.scaleInRequirements.contains(component);
	}
	
	/**
	 * @return the activityValues
	 */
	public HashMap<String, Double> getActivityValues() {
		return activityValues;
	}

	/**
	 * @param activityValues the activityValues to set
	 */
	public void setActivityValues(HashMap<String, Double> activityValues) {
		this.activityValues = activityValues;
	}

	public Double getActivityValue(String component){
		return this.activityValues.get(component);
	}
	
	public Integer getSamplingRate(){
		return this.samplingRate;
	}
	
	public boolean isInputDecreasing(String component){
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		Double coeff = RegressionTools.linearRegressionCoeff(inputRecords);
		return (coeff < -VAR_THRESHOLD);
	}
	
	public boolean isInputStable(String component){
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		Double coeff = RegressionTools.linearRegressionCoeff(inputRecords);
		return (coeff >= -VAR_THRESHOLD && coeff <= VAR_THRESHOLD);
	}
	
	public boolean isInputIncreasing(String component){
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		Double coeff = RegressionTools.linearRegressionCoeff(inputRecords);
		return (coeff > VAR_THRESHOLD);
	}
	
	public HashMap<String, Long> getFormerRemainingTuples(TopologyExplorer explorer){
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
	
	public void buildActionGraph(TopologyExplorer explorer, AssignmentMonitor assignmentMonitor){
		//Initialize an activity and load metric for the current topology
		ActivityMetric activityMetric = new ActivityMetric(this, assignmentMonitor, explorer);
		LoadMetric loadMetric = new LoadMetric(this, assignmentMonitor);
		for(String component : this.getRegisteredComponents()){
			if(hasRecords(component)){
				//Compute the activity level/ load and expose monitoring info concerning the activity/ load for storage
				Double activityValue = activityMetric.compute(component);
				this.activityValues.put(component, activityValue);
				HashMap<String, BigDecimal> activityInfo = activityMetric.getActivityInfo(component);
				this.manager.storeActivityInfo(this.timestamp, explorer.getTopologyName(), component, activityValue, activityInfo.get(ActivityMetric.REMAINING).intValue(), activityInfo.get(ActivityMetric.CAPPERSEC).doubleValue());
				
				Double loadValue = loadMetric.compute(component);
				this.loadValues.put(component, loadValue);
				HashMap<String, BigDecimal> loadInfo = loadMetric.getLoadInfo(component);
				this.manager.storeLoadInfo(this.timestamp, explorer.getTopologyName(), component, loadValue, loadInfo.get(LoadMetric.LOAD).doubleValue());
				//Apply rules to take local decisions
				if(activityValue <= LOW_ACTIVITY_THRESHOLD && !isInputIncreasing(component)){
					this.scaleInRequirements.add(component);
					//System.out.println("Timestamp: " + this.timestamp + " Component " + component + " required scale in. Activity value: " + activityValue + " and input decreasing or constant.");
				}else{
					if(activityValue > HIGH_ACTIVITY_THRESHOLD && activityValue <= 1 && isInputIncreasing(component)){
						this.scaleOutRequirements.add(component);
						//System.out.println("Timestamp: " + this.timestamp + " Component " + component + " required scale out. Activity value: " + activityValue + " and input increasing.");
					}else{
						if(activityValue > 1){
							this.scaleOutRequirements.add(component);
							//System.out.println("Timestamp: " + this.timestamp + " Component " + component + " required scale out. Activity value: " + activityValue);
						}
					}
				}
			}
		}
	}
	
	public boolean validateScaleIn(String component, TopologyExplorer explorer){
		boolean validate = false;
		Double activityValue = this.getActivityValue(component);
		//System.out.println(component + " has epr = " + activityValue);
		if(activityValue != null){
			if(activityValue == -1.0){
				//System.out.println("Timestamp: " + this.timestamp + " Component " + component + " cannot scale in because of epr = -1");
				return false;
			}
			ArrayList<String> antecedents = explorer.getAntecedents(component);
			for(String antecedent : antecedents){
				Double antecedentActivivityValue = this.getActivityValue(antecedent);
				if(antecedentActivivityValue != null){
					if(antecedentActivivityValue >= 1){
						//System.out.println("Timestamp: " + this.timestamp + " Component " + component + " cannot scale in because " + antecedent + " can scale out");
						return false;
					}
				}
			}
			if(this.needScaleIn(component)){
				//System.out.println("Timestamp: " + this.timestamp + " Component " + component + " can scale in");
				validate = true;
			}
		}
		return validate;
	}
	
	public boolean validateScaleOut(String component, TopologyExplorer explorer){
		boolean validate = false;
		Double activityValue = this.getActivityValue(component);
		//System.out.println(component + " has epr = " + eprValue);
		if(activityValue != null){
			if(activityValue == -1.0){
				//logger.info("Timestamp: " + this.timestamp + " Component " + component + " cannot scale out because of epr = -1");
				return false;
			}
			ArrayList<String> parents = explorer.getParents(component);
			for(String parent : parents){
				if(this.validateScaleOut(parent, explorer)){
					//logger.info("Timestamp: " + this.timestamp + " Component " + component + " will scale out instead remaining in the same state because " + parent + " will scale out");
					validate = true;
					break;
				}
			}
			if(this.needScaleOut(component)){
				validate = true;
			}
			
		}
		return validate;
	}
	
	public void autoscaleAlgorithm(ArrayList<String> parents, TopologyExplorer explorer){
		for(String parent : parents){
			ArrayList<String> children = explorer.getChildren(parent);
			for(String child : children){
				if(this.needScaleIn(child)){
					boolean validate = this.validateScaleIn(child, explorer);
					if(!validate){
						this.scaleInRequirements.remove(child);
					}
				}else{
					boolean validate = this.validateScaleOut(child, explorer);
					if(!validate){
						this.scaleOutRequirements.remove(child);
					}
				}
			}
			for(String child : children){
				ArrayList<String> grandChildren = explorer.getChildren(child);
				if(!grandChildren.isEmpty()){
					autoscaleAlgorithm(children, explorer);
				}
			}
		}
	}
	
	public void reset(){
		this.stats = new HashMap<>();
	}
}