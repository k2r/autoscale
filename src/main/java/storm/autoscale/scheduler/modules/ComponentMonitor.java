/**
 * 
 */
package storm.autoscale.scheduler.modules;

//import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
//import java.util.logging.FileHandler;
import java.util.logging.Logger;
//import java.util.logging.SimpleFormatter;

import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.metrics.ActivityMetric;
import storm.autoscale.scheduler.metrics.IMetric;
import storm.autoscale.scheduler.metrics.ImpactMetric;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.regression.LinearRegressionTools;

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
	private HashMap<String, Double> activityValues;
	private HashMap<String, Double> estimatedLoads;
	private HashMap<String, Double> capacities;
	private HashMap<String, Integer> degrees; 
	
	private HashMap<String, Integer> scaleOutActions;
	private HashMap<String, Integer> scaleInActions;
	private HashMap<String, Integer> nothingActions;
	
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
		this.activityValues = new HashMap<>();
		this.estimatedLoads = new HashMap<>();
		this.capacities = new HashMap<>();
		this.degrees = new HashMap<>();
		this.scaleOutActions = new HashMap<>();
		this.scaleInActions = new HashMap<>();
		this.nothingActions = new HashMap<>();
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

	public Double getActivityValue(String component){
		return this.activityValues.get(component);
	}
	
	public Double getEstimatedLoad(String component){
		return this.estimatedLoads.get(component);
	}
	
	public Double getCapacity(String component){
		return this.capacities.get(component);
	}
	
	public Integer getCurrentDegree(String component){
		return this.degrees.get(component);
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
	
	/**
	 * @return the scaleOutActions
	 */
	public HashMap<String, Integer> getScaleOutActions() {
		return scaleOutActions;
	}

	/**
	 * @return the scaleInActions
	 */
	public HashMap<String, Integer> getScaleInActions() {
		return scaleInActions;
	}
	
	/**
	 * @return the degrees
	 */
	public HashMap<String, Integer> getDegrees() {
		return degrees;
	}

	/**
	 * @param degrees the degrees to set
	 */
	public void setDegrees(HashMap<String, Integer> degrees) {
		this.degrees = degrees;
	}

	/**
	 * @return the nothingActions
	 */
	public HashMap<String, Integer> getNothingActions() {
		return nothingActions;
	}

	/**
	 * @param nothingActions the nothingActions to set
	 */
	public void setNothingActions(HashMap<String, Integer> nothingActions) {
		this.nothingActions = nothingActions;
	}

	/**
	 * @param scaleOutActions the scaleOutActions to set
	 */
	public void setScaleOutActions(HashMap<String, Integer> scaleOutActions) {
		this.scaleOutActions = scaleOutActions;
	}

	/**
	 * @param scaleInActions the scaleInActions to set
	 */
	public void setScaleInActions(HashMap<String, Integer> scaleInActions) {
		this.scaleInActions = scaleInActions;
	}

	public boolean isInputDecreasing(String component){
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		Double coeff = LinearRegressionTools.linearRegressionCoeff(inputRecords);
		Double decreaseThreshold = this.parser.getSlopeThreshold() * -1.0;
		return (coeff < decreaseThreshold);
	}
	
	public boolean isInputStable(String component){
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		Double coeff = LinearRegressionTools.linearRegressionCoeff(inputRecords);
		Double decreaseThreshold = this.parser.getSlopeThreshold() * -1.0;
		Double increaseThreshold = this.parser.getSlopeThreshold();
		return (coeff >= decreaseThreshold && coeff <= increaseThreshold);
	}
	
	public boolean isInputIncreasing(String component){
		HashMap<Integer, Long> inputRecords = this.getStats(component).getInputRecords();
		Double coeff = LinearRegressionTools.linearRegressionCoeff(inputRecords);
		Double increaseThreshold = this.parser.getSlopeThreshold();
		return (coeff > increaseThreshold);
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
	
	public void buildDegreeMap(AssignmentMonitor assignmentMonitor){
		Set<String> components = this.getRegisteredComponents();
		for(String component : components){
			Integer degree = assignmentMonitor.getParallelism(component);
			if(degree > 0){
				this.degrees.put(component, degree);
			}
		}
	}
	
	public void buildActionGraph(IMetric metric, AssignmentMonitor assignmentMonitor){
		Double lowActivityThreshold = this.parser.getLowActivityThreshold();
		Double highActivityThreshold = this.parser.getHighActivityThreshold();
		logger.fine("low threshold: " + lowActivityThreshold + ", high threshold: " + highActivityThreshold);
		ActivityMetric activityMetric = (ActivityMetric) metric;//cast to the metric you want
		//Initialize an activity metric for the current topology
		for(String component : this.getRegisteredComponents()){
			logger.fine("Looking for component " + component);
			if(hasRecords(component)){
				logger.fine("Evaluating scaling requirement for component " + component);
				//Compute the activity level and expose monitoring info concerning the activity for storage
				Double activityValue = metric.compute(component);
				logger.fine("Component " + component + " has activity value: " + activityValue);
				this.activityValues.put(component, activityValue);
				HashMap<String, BigDecimal> activityInfo = activityMetric.getActivityInfo(component);
				this.manager.storeActivityInfo(this.timestamp, activityMetric.getTopologyExplorer().getTopologyName(), component, activityValue,
						activityInfo.get(ActivityMetric.REMAINING).intValue(),
						activityInfo.get(ActivityMetric.CAPPERSEC).doubleValue(),
						activityInfo.get(ActivityMetric.ESTIMLOAD).doubleValue());
		
				this.estimatedLoads.put(component, activityInfo.get(ActivityMetric.ESTIMLOAD).doubleValue());
				this.capacities.put(component, activityInfo.get(ActivityMetric.CAPPERSEC).doubleValue());
				//Compute the adequate parallelism degree thanks to local (activity level) estimations
				Integer maxParallelism = assignmentMonitor.getAllSortedTasks(component).size();
				Integer currentParallelism = this.getCurrentDegree(component);
				Integer estimatedParallelism = Math.max(1, (int) Math.round(currentParallelism * activityValue));
				Integer degree = (Integer) Math.min(maxParallelism, estimatedParallelism);
				
				logger.fine("Component " + component + ": ");
				logger.fine("Current degree: " + currentParallelism);
				logger.fine("Estimated degree: " + estimatedParallelism);
				logger.fine("Max degree: " + maxParallelism);
				logger.fine("Adequate degree " + degree);
				//Apply rules to take local decisions
				if(activityValue <= lowActivityThreshold && activityValue != -1.0 && !isInputIncreasing(component)){
					this.scaleInActions.put(component, degree);
					logger.fine("Component " + component + " required a scale-in to degree " + degree);
				}else{
					if(activityValue > highActivityThreshold && activityValue <= 1 && isInputIncreasing(component)){
						degree++;
						this.scaleOutActions.put(component, degree);
						logger.fine("Component " + component + " required a scale-out to degree " + degree);
					}else{
						if(activityValue > 1){
							this.scaleOutActions.put(component, degree);
							logger.fine("Component " + component + " required a scale-out to degree " + degree);
						}else{
							this.nothingActions.put(component, currentParallelism);
							logger.fine("Component " + component + " required no action");
						}
					}
				}
			}
		}
	}
	
	

	public void autoscaleAlgorithm(HashSet<String> ancestors, TopologyExplorer explorer){
		HashSet<String> descendants = new HashSet<>();
		HashSet<String> checkedComponents = new HashSet<>();
		for(String ancestor : ancestors){
			System.out.println("Starting from ancestor component " + ancestor);
			ArrayList<String> children = explorer.getChildren(ancestor);
			descendants.addAll(children);
			boolean isAncestorCritical = this.scaleOutActions.containsKey(ancestor);
			for(String child : children){
				System.out.println("Checking global consistency for child component " + child);
				if(isAncestorCritical && !checkedComponents.contains(child)){
					boolean isChildUnderUsed = this.scaleInActions.containsKey(child);
					boolean isChildRegularUsed = this.nothingActions.containsKey(child);
					boolean isChildCritical = this.scaleOutActions.containsKey(child);
					if(isChildRegularUsed){
						Integer currentDegree = this.nothingActions.remove(child);
						currentDegree++;
						this.scaleOutActions.put(child, currentDegree);
						checkedComponents.add(child);
						System.out.println("Component " + child + " moved from nothing to scale-out with degree "  + currentDegree);
					}else{
						if(isChildUnderUsed){
							this.scaleInActions.remove(child);
							this.nothingActions.put(child, this.getCurrentDegree(child));
							checkedComponents.add(child);
							System.out.println("Component " + child + " moved from scale-in to nothing with degree " + this.getCurrentDegree(child));
						}else{
							if(isChildCritical){
								Integer adequateDegree = this.scaleOutActions.remove(child);
								adequateDegree++;
								this.scaleOutActions.put(child, adequateDegree);
								checkedComponents.add(child);
								System.out.println("Component " + child + " reevaluated for scale-out with new degree " + adequateDegree);
							}
						}
					}
				}
			}
		} 
		if(!descendants.isEmpty()){
			autoscaleAlgorithm(descendants, explorer);
		}
	}
	
	public void autoscaleAlgorithmWithImpact(HashSet<String> ancestors, TopologyExplorer explorer, AssignmentMonitor assignMonitor){
		for(String ancestor : ancestors){
			ArrayList<String> children = explorer.getChildren(ancestor);
			boolean isAncestorCritical = this.scaleOutActions.containsKey(ancestor);
			if(isAncestorCritical){
				for(String child : children){
					boolean isChildUnderUsed = this.scaleInActions.containsKey(child);
					boolean isChildRegularUsed = this.nothingActions.containsKey(child);
					boolean isChildCritical = this.scaleOutActions.containsKey(child);
					
					ImpactMetric impact = new ImpactMetric(this, explorer);
					Double impactValue = impact.compute(child);
					Integer currentDegree = this.getCurrentDegree(child);
					Integer impactDegree = impact.getImpactDegrees().get(child);
					Integer maxParallelism = assignMonitor.getAllSortedTasks(child).size();
					
					if(isChildRegularUsed){
						Integer adequateDegree = Math.max(currentDegree, impactDegree);//It could also be Math.min depending on user strategy
						adequateDegree = Math.max(maxParallelism, adequateDegree);//to ask only for feasible scale-out actions
						if(adequateDegree > currentDegree){
							this.nothingActions.remove(child);
							this.scaleOutActions.put(child, adequateDegree);
							this.estimatedLoads.put(child, impactValue);// to propagate the effect on next components						
						}
					}else{
						if(isChildUnderUsed){
							Integer localRequiredDegree = this.scaleInActions.get(child);
							Integer adequateDegree = Math.max(localRequiredDegree, impactDegree);//It could also be Math.min depending on user strategy
							adequateDegree = Math.max(maxParallelism, adequateDegree);//to ask only for feasible scale-out actions
							if(adequateDegree < currentDegree){//the impact confirms a scale-in action, we just set the degree
								this.scaleInActions.put(child, adequateDegree);
							}else{
								if(adequateDegree == currentDegree){
									this.scaleInActions.remove(child);
									this.nothingActions.put(child, adequateDegree);//the impact turns a scale-in into a nothing action, so we cancel the action
								}else{
									if(adequateDegree > currentDegree){
										this.scaleInActions.remove(child);
										this.scaleOutActions.put(child, adequateDegree);
									}
								}
							}
						}else{
							if(isChildCritical){
								Integer localRequiredDegree = this.scaleOutActions.get(child);
								Integer adequateDegree = Math.max(localRequiredDegree, impactDegree);//It could also be Math.min depending on user strategy
								adequateDegree = Math.max(maxParallelism, adequateDegree);//to ask only for feasible scale-out actions
								if(adequateDegree > localRequiredDegree){
									this.scaleOutActions.put(child, adequateDegree);//considering the impact confirms the scale-out and we consider a max strategy all we need is to now if we have to revise the degree
								}
							}
						}
					}
					
				}
			}
		}
	}
	
	public void reset(){
		this.stats = new HashMap<>();
	}
}