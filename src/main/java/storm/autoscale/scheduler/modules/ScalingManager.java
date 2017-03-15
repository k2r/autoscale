/**
 * 
 */
package storm.autoscale.scheduler.modules;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.storm.Config;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.Component;

import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.metrics.ActivityMetric;
import storm.autoscale.scheduler.metrics.IMetric;
import storm.autoscale.scheduler.metrics.ImpactMetric;
import storm.autoscale.scheduler.regression.LinearRegressionTools;
import storm.autoscale.scheduler.regression.RegressionSelector;

/**
 * @author Roland
 *
 */
public class ScalingManager {

	private ComponentMonitor monitor; 
	private XmlConfigParser parser;
	
	private HashMap<String, Integer> degrees; 
	private HashMap<String, Double> activityValues;
	private HashMap<String, Double> estimatedLoads;
	private HashMap<String, Double> capacities;
	
	private HashMap<String, Integer> scaleOutActions;
	private HashMap<String, Integer> scaleInActions;
	private HashMap<String, Integer> nothingActions;

	private static Logger logger = Logger.getLogger("ScalingManager");
	
	public ScalingManager(ComponentMonitor monitor, XmlConfigParser parser) {
		this.monitor = monitor;
		this.parser = parser;
		this.degrees = new HashMap<>();
		this.activityValues = new HashMap<>();
		this.estimatedLoads = new HashMap<>();
		this.capacities = new HashMap<>();
		this.scaleOutActions = new HashMap<>();
		this.scaleInActions = new HashMap<>();
		this.nothingActions = new HashMap<>();	
	}
	
	/**
	 * @return the monitor
	 */
	public ComponentMonitor getMonitor() {
		return monitor;
	}

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(ComponentMonitor monitor) {
		this.monitor = monitor;
	}

	/**
	 * @return the parser
	 */
	public XmlConfigParser getParser() {
		return parser;
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
	
	public Integer getDegree(String component){
		return this.degrees.get(component);
	}
	
	public Double getActivityValue(String component){
		return this.activityValues.get(component);
	}
	
	public Double getEstimatedLoad(String component){
		return this.estimatedLoads.get(component);
	}
	
	/**
	 * @return the capacities
	 */
	public HashMap<String, Double> getCapacities() {
		return capacities;
	}
	
	public void setCapacities(HashMap<String, Double> capacities) {
		this.capacities = capacities;
	}
	
	public Double getCapacity(String component) {
		return this.capacities.get(component);
	}

	/**
	 * @return the scaleOutActions
	 */
	public HashMap<String, Integer> getScaleOutActions() {
		return scaleOutActions;
	}

	/**
	 * @param scaleOutActions the scaleOutActions to set
	 */
	public void setScaleOutActions(HashMap<String, Integer> scaleOutActions) {
		this.scaleOutActions = scaleOutActions;
	}

	/**
	 * @return the scaleInActions
	 */
	public HashMap<String, Integer> getScaleInActions() {
		return scaleInActions;
	}

	/**
	 * @param scaleInActions the scaleInActions to set
	 */
	public void setScaleInActions(HashMap<String, Integer> scaleInActions) {
		this.scaleInActions = scaleInActions;
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
	
	public void setConstraints(TopologyDetails topology, String component, Double cpu, Double memory){
		Component comp = topology.getComponents().get(component);
		List<ExecutorDetails> executors = comp.execs;
		HashMap<String, Double> constraints = new HashMap<>();
		constraints.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, cpu);
		constraints.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, memory);
		for(ExecutorDetails exec : executors){
			topology.addResourcesForExec(exec, constraints);
		}
	}
	
	public boolean isInputDecreasing(String component){
		HashMap<Integer, Long> inputRecords = this.monitor.getStats(component).getInputRecords();
		Double coeff = LinearRegressionTools.regressionCoeff(inputRecords);
		Double decreaseThreshold = this.parser.getSlopeThreshold() * -1.0;
		return (coeff < decreaseThreshold);
	}
	
	public boolean isInputStable(String component){
		HashMap<Integer, Long> inputRecords = this.monitor.getStats(component).getInputRecords();
		Double coeff = LinearRegressionTools.regressionCoeff(inputRecords);
		Double decreaseThreshold = this.parser.getSlopeThreshold() * -1.0;
		Double increaseThreshold = this.parser.getSlopeThreshold();
		return (coeff >= decreaseThreshold && coeff <= increaseThreshold);
	}
	
	public boolean isInputIncreasing(String component){
		HashMap<Integer, Long> inputRecords = this.monitor.getStats(component).getInputRecords();
		Double coeff = LinearRegressionTools.regressionCoeff(inputRecords);
		Double increaseThreshold = this.parser.getSlopeThreshold();
		return (coeff > increaseThreshold);
	}
	
	public Double getEstimatedCpuUsagePerExec(String component){
		HashMap<Integer, Double> avgCpuRecords = monitor.getStats(component).getAvgCpuUsageRecords(); // we consider the average cpu usage of all executors associated to the component

		Integer timestamp = this.monitor.getTimestamp();
		Integer step = this.monitor.getMonitoringFrequency();
		
		RegressionSelector<Integer, Double> selector = new RegressionSelector<>(avgCpuRecords); // we perform a best-effort regression of average cpu usage to estimate the average one on the next observation
		return selector.estimateYCoordinate(timestamp + step);
	}
	
	public void buildDegreeMap(AssignmentMonitor assignmentMonitor){
		Set<String> components = this.monitor.getRegisteredComponents();
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
		for(String component : this.monitor.getRegisteredComponents()){
			logger.fine("Looking for component " + component);
			if(this.monitor.hasRecords(component)){
				logger.fine("Evaluating scaling requirement for component " + component);
				//Compute the activity level and expose monitoring info concerning the activity for storage
				Double activityValue = metric.compute(component);
				logger.fine("Component " + component + " has activity value: " + activityValue);
				this.activityValues.put(component, activityValue);
				HashMap<String, BigDecimal> activityInfo = activityMetric.getActivityInfo(component);
				this.monitor.getManager().storeActivityInfo(this.monitor.getTimestamp(), activityMetric.getTopologyExplorer().getTopologyName(), component, activityValue,
						activityInfo.get(ActivityMetric.REMAINING).intValue(),
						activityInfo.get(ActivityMetric.CAPPERSEC).doubleValue(),
						activityInfo.get(ActivityMetric.ESTIMLOAD).doubleValue());

				this.estimatedLoads.put(component, activityInfo.get(ActivityMetric.ESTIMLOAD).doubleValue());
				this.capacities.put(component, activityInfo.get(ActivityMetric.CAPPERSEC).doubleValue());
				//Compute the adequate parallelism degree thanks to local (activity level) estimations
				Integer maxParallelism = assignmentMonitor.getAllSortedTasks(component).size();
				Integer currentParallelism = this.getDegree(component);
				Integer estimatedParallelism = Math.max(1, (int) Math.round(currentParallelism * activityValue));
				Integer degree = (Integer) Math.min(maxParallelism, estimatedParallelism);

				logger.fine("Component " + component + ": ");
				logger.fine("Current degree: " + currentParallelism);
				logger.fine("Estimated degree: " + estimatedParallelism);
				logger.fine("Max degree: " + maxParallelism);
				logger.fine("Adequate degree " + degree);
				//Apply rules to take local decisions
				if(activityValue <= lowActivityThreshold && activityValue != -1.0 && !isInputIncreasing(component) && degree < currentParallelism){
					this.scaleInActions.put(component, degree);
					logger.fine("Component " + component + " required a scale-in to degree " + degree);
				}else{
					if(activityValue > highActivityThreshold && activityValue <= 1 && isInputIncreasing(component)){
						degree++;
						if(degree > currentParallelism){
							this.scaleOutActions.put(component, degree);
							logger.fine("Component " + component + " required a scale-out to degree " + degree);
						}
					}else{
						if(activityValue > 1 && degree > currentParallelism){
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
			logger.fine("Starting from ancestor component " + ancestor);
			ArrayList<String> children = explorer.getChildren(ancestor);
			descendants.addAll(children);
			boolean isAncestorCritical = this.scaleOutActions.containsKey(ancestor);
			for(String child : children){
				logger.fine("Checking global consistency for child component " + child);
				if(isAncestorCritical && !checkedComponents.contains(child)){
					boolean isChildUnderUsed = this.scaleInActions.containsKey(child);
					boolean isChildRegularUsed = this.nothingActions.containsKey(child);
					boolean isChildCritical = this.scaleOutActions.containsKey(child);
					if(isChildRegularUsed){
						Integer currentDegree = this.nothingActions.remove(child);
						currentDegree++;
						this.scaleOutActions.put(child, currentDegree);
						checkedComponents.add(child);
						logger.fine("Component " + child + " moved from nothing to scale-out with degree "  + currentDegree);
					}else{
						if(isChildUnderUsed){
							this.scaleInActions.remove(child);
							this.nothingActions.put(child, this.getDegree(child));
							checkedComponents.add(child);
							logger.fine("Component " + child + " moved from scale-in to nothing with degree " + this.getDegree(child));
						}else{
							if(isChildCritical){
								Integer adequateDegree = this.scaleOutActions.remove(child);
								adequateDegree++;
								this.scaleOutActions.put(child, adequateDegree);
								checkedComponents.add(child);
								logger.fine("Component " + child + " reevaluated for scale-out with new degree " + adequateDegree);
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
	
	public void autoscaleAlgorithmWithImpact(IMetric metric, HashSet<String> ancestors, TopologyExplorer explorer, AssignmentMonitor assignMonitor){
		HashSet<String> descendants = new HashSet<>();
		HashSet<String> checkedComponents = new HashSet<>();
		for(String ancestor : ancestors){
			logger.fine("Starting from ancestor component " + ancestor);
			ArrayList<String> children = explorer.getChildren(ancestor);
			descendants.addAll(children);
			boolean isAncestorCritical = this.scaleOutActions.containsKey(ancestor);

			for(String child : children){
				logger.fine("Checking global consistency for child component " + child);
				if(isAncestorCritical && !checkedComponents.contains(child)){
					boolean isChildUnderUsed = this.scaleInActions.containsKey(child);
					boolean isChildRegularUsed = this.nothingActions.containsKey(child);
					boolean isChildCritical = this.scaleOutActions.containsKey(child);

					ImpactMetric impactMetric = (ImpactMetric) metric;//could be replaced by any metric for global consistency
					Double impactValue = impactMetric.compute(child);
					Integer currentDegree = this.getDegree(child);
					Integer impactDegree = impactMetric.getImpactDegrees().get(child);
					Integer maxParallelism = assignMonitor.getAllSortedTasks(child).size();
					Integer capacityPerWindow = (int) Math.round(this.capacities.get(child) * this.getParser().getWindowSize());

					if(isChildRegularUsed){
						Integer adequateDegree = Math.max(currentDegree, impactDegree);//It could also be Math.min depending on user strategy
						adequateDegree = Math.min(maxParallelism, adequateDegree);//to ask only for feasible scale-out actions
						if(adequateDegree > currentDegree){
							this.nothingActions.remove(child);
							this.scaleOutActions.put(child, adequateDegree);
							logger.fine("Component " + child + " moved from nothing to scale-out with degree "  + adequateDegree);
							checkedComponents.add(child);
							this.estimatedLoads.put(child, Math.min(impactValue, capacityPerWindow));// to propagate the effect on next components						
						}
					}else{
						if(isChildUnderUsed){
							Integer localRequiredDegree = this.scaleInActions.get(child);
							Integer adequateDegree = Math.max(localRequiredDegree, impactDegree);//It could also be Math.min depending on user strategy
							adequateDegree = Math.min(maxParallelism, adequateDegree);//to ask only for feasible scale-out actions
							if(adequateDegree < currentDegree){//the impact confirms a scale-in action, we just set the degree
								this.scaleInActions.put(child, adequateDegree);
								logger.fine("Component " + child + " reevaluated for scale-in with new degree " + adequateDegree);
								checkedComponents.add(child);
							}else{
								if(adequateDegree == currentDegree){
									this.scaleInActions.remove(child);
									this.nothingActions.put(child, adequateDegree);//the impact turns a scale-in into a nothing action, so we cancel the action
									logger.fine("Component " + child + " moved from scale-in to nothing with degree " + adequateDegree);
									checkedComponents.add(child);
								}else{
									if(adequateDegree > currentDegree){
										this.scaleInActions.remove(child);
										this.scaleOutActions.put(child, adequateDegree);
										logger.fine("Component " + child + " moved from scale-in to scale-out with degree " + adequateDegree);
										checkedComponents.add(child);
									}
								}
							}
						}else{
							if(isChildCritical){
								Integer localRequiredDegree = this.scaleOutActions.get(child);
								Integer adequateDegree = Math.max(localRequiredDegree, impactDegree);//It could also be Math.min depending on user strategy
								adequateDegree = Math.min(maxParallelism, adequateDegree);//to ask only for feasible scale-out actions
								if(adequateDegree > localRequiredDegree){
									this.scaleOutActions.put(child, adequateDegree);//considering the impact confirms the scale-out and we consider a max strategy all we need is to now if we have to revise the degree
									logger.fine("Component " + child + " reevaluated for scale-out with new degree " + adequateDegree);
									checkedComponents.add(child);
								}
							}
						}
					}

				}
			}
		}
		if(!descendants.isEmpty()){
			autoscaleAlgorithmWithImpact(metric, descendants, explorer, assignMonitor);
		}
	}
	
	public void adjustDegreesToCpuConstraint(TopologyExplorer explorer){
		HashMap<String, Double> cpuConstraints = this.monitor.getCurrentCpuConstraints(explorer);
		for(String component : this.scaleOutActions.keySet()){
			Integer degree = this.scaleOutActions.get(component);
			Double cpuConstraint = cpuConstraints.get(component);
			Double cpuEstimatedUsage = this.getEstimatedCpuUsagePerExec(component);
			if(cpuConstraint < cpuEstimatedUsage){
				Integer cpuAwareDegree = (int) Math.round(degree * (cpuEstimatedUsage / cpuConstraint));
				this.scaleOutActions.put(component, cpuAwareDegree);
			}
		}
	}
}