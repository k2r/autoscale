/**
 * 
 */
package storm.autoscale.scheduler.modules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Logger;

import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.regression.RegressionSelector;

/**
 * @author Roland
 *
 */
public class ScalingManager3 {

    private ComponentMonitor cm;
    private TopologyExplorer explorer;
    private AssignmentMonitor am;
	private XmlConfigParser parser;
	
	private HashMap<String, Double> estimInput;
	private HashMap<String, Double> estimMaxCap;
	private HashMap<String, Double> utilCPU;
	
	private HashMap<String, Integer> newDegrees;
	
	private static Logger logger = Logger.getLogger("ScalingManager3");
	
	public ScalingManager3(ComponentMonitor cm, TopologyExplorer explorer, AssignmentMonitor am, XmlConfigParser parser) {
		this.cm = cm;
		this.explorer = explorer;
		this.am = am;
		this.parser = parser;
		this.estimInput = new HashMap<>();
		this.estimMaxCap = new HashMap<>();
		this.utilCPU = new HashMap<>();
		this.newDegrees = new HashMap<>();
	}
	
	public void computeEstimInputs(){
		
		ArrayList<Integer> timestamps = new ArrayList<>();
		Integer currTime = this.cm.getTimestamp();
		Integer freq = this.cm.getMonitoringFrequency();
		Integer windowSize = this.parser.getWindowSize();
		Integer endNextWindow = currTime + windowSize;
		for(int i = currTime; i <= endNextWindow; i += freq){
			timestamps.add(i + freq);
		}
		
		HashSet<String> ancestors = this.explorer.getAncestors();
		//TODO do the BFS on ancestors 
	}
	
	public void computeEstimInputs(HashSet<String> components, ArrayList<Integer> timestamps){
		for(String component : components){
			HashMap<Integer, Long> inputs = this.cm.getStats(component).getInputRecords();
			RegressionSelector<Integer, Long> regression = new RegressionSelector<>(inputs);
			Double estimR = 0.0;
			for(Integer timestamp : timestamps){
				estimR += regression.estimateYCoordinate(timestamp);
			}
			
			Double estimParentOutputs = 0.0;
			ArrayList<String> parents = this.explorer.getParents(component);
			if(!parents.isEmpty()){
				for(String parent : parents){
					HashMap<Integer, Long> parentInputs = this.cm.getStats(parent).getInputRecords();
					RegressionSelector<Integer, Long> parentRegression = new RegressionSelector<>(parentInputs);
					Double estimParentR = 0.0;
					for(Integer timestamp : timestamps){
						estimParentR += parentRegression.estimateYCoordinate(timestamp);
					}
					Double parentSelectivity = ComponentWindowedStats.getLastRecord(this.cm.getStats(parent).getSelectivityRecords());
					Double estimParentOutput = estimParentR * parentSelectivity;
					estimParentOutputs += estimParentOutput;
				}
			}
			
			Long pending = this.cm.getPendingTuples(this.explorer).get(component);
			Double estimInput = combine(estimR, estimParentOutputs) + pending; 
			this.estimInput.put(component, estimInput);
		}
		
		//TODO gather all children and call this method on them recursively
	}
	
	public void computeEstimMaxCapacities(){
		
	}
	
	public void computeUtilCPU(){
		
	}
	
	public void computeNewDegrees(){
		
	}
	
	public boolean needScaleOut(String component){
		return (getEstimInput(component) / (getEstimMaxCapacity(component) * getUtilCPU(component))) >= 1; 
	}
	
	public boolean needScaleIn(String component){
		Double thetaMin = this.parser.getLowActivityThreshold();
		return (getEstimInput(component) / (getEstimMaxCapacity(component) * getUtilCPU(component))) < thetaMin;
	}
	
	public Double combine(Double estim1, Double estim2){
		return Math.max(estim1, estim2);
	}
	
	public Double getEstimInput(String component){
		return this.estimInput.get(component);
	}
	
	public Double getEstimMaxCapacity(String component){
		return this.estimMaxCap.get(component);
	}
	
	public Double getUtilCPU(String component){
		return this.utilCPU.get(component);
	}
	
	public Integer getNewDegree(String component){
		return this.newDegrees.get(component);
	}
}