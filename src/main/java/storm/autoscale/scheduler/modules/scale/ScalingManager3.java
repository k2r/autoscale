/**
 * 
 */
package storm.autoscale.scheduler.modules.scale;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.storm.scheduler.WorkerSlot;

import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.modules.assignment.AssignmentMonitor;
import storm.autoscale.scheduler.modules.component.ComponentMonitor;
import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.regression.RegressionSelector;
import storm.autoscale.scheduler.util.UtilFunctions;

/**
 * @author Roland
 *
 */
public class ScalingManager3 {
	
	private HashMap<String, Double> estimInput;
	private HashMap<String, Double> estimMaxCap;
	private HashMap<String, Double> utilCPU;
	private HashMap<String, Integer> degrees;
	
	
	private HashMap<String, Integer> scaleInActions;
	private HashMap<String, Integer> scaleOutActions;
	
	private static Logger logger = Logger.getLogger("ScalingManager3");
	
	public ScalingManager3() {
		logger.fine("Evaluating scaling requirements on recent history...");
		this.estimInput = new HashMap<>();
		this.estimMaxCap = new HashMap<>();
		this.utilCPU = new HashMap<>();
		this.degrees = new HashMap<>();
		this.scaleInActions = new HashMap<>();
		this.scaleOutActions = new HashMap<>();
	}
	
	public void initDegrees(ComponentMonitor cm, AssignmentMonitor am){
		Set<String> components = cm.getRegisteredComponents();
		for(String component : components){
			this.degrees.put(component, am.getParallelism(component));
		}
	}
	
	public void computeEstimInputs(ComponentMonitor cm, TopologyExplorer explorer){
		
		ArrayList<Integer> timestamps = new ArrayList<>();
		Integer currTime = cm.getTimestamp();
		Integer freq = cm.getMonitoringFrequency();
		Integer windowSize = cm.getParser().getWindowSize();
		Integer endNextWindow = currTime + windowSize;
		for(int i = currTime; i <= endNextWindow; i += freq){
			timestamps.add(i + freq);
		}
		
		HashSet<String> ancestors = explorer.getAncestors();
		computeEstimInputs(ancestors, timestamps, cm, explorer); 
	}
	
	public void computeEstimInputs(HashSet<String> components, ArrayList<Integer> timestamps, ComponentMonitor cm, TopologyExplorer explorer){
		HashSet<String> allChildren = new HashSet<>();
		for(String component : components){
			HashMap<Integer, Long> inputs = cm.getStats(component).getInputRecords();
			RegressionSelector<Integer, Long> regression = new RegressionSelector<>(inputs);
			Double estimR = 0.0;
			for(Integer timestamp : timestamps){
				estimR += regression.estimateYCoordinate(timestamp);
			}
			
			Double estimParentOutputs = 0.0;
			ArrayList<String> parents = explorer.getParents(component);
			if(!parents.isEmpty()){
				for(String parent : parents){
					HashMap<Integer, Long> parentInputs = cm.getStats(parent).getInputRecords();
					RegressionSelector<Integer, Long> parentRegression = new RegressionSelector<>(parentInputs);
					Double estimParentR = 0.0;
					for(Integer timestamp : timestamps){
						estimParentR += parentRegression.estimateYCoordinate(timestamp);
					}
					Double parentSelectivity = ComponentWindowedStats.getLastRecord(cm.getStats(parent).getSelectivityRecords());
					Double estimParentOutput = estimParentR * parentSelectivity;
					estimParentOutputs += estimParentOutput;
				}
			}
			
			Long pending = cm.getPendingTuples(explorer).get(component);
			Double estimInput = combine(estimR, estimParentOutputs) + pending; 
			this.estimInput.put(component, estimInput);
			
			ArrayList<String> children = explorer.getChildren(component);
			allChildren.addAll(children);
		}
		if(!allChildren.isEmpty()){
			computeEstimInputs(allChildren, timestamps, cm, explorer);
		}
	}
	
	public void computeEstimMaxCapacities(ComponentMonitor cm){
		Set<String> components = cm.getRegisteredComponents();
		Integer delta = cm.getParser().getWindowSize();
		for(String component : components){
			HashMap<Integer, Double> latencyRecords = cm.getStats(component).getAvgLatencyRecords();
			
			
			ArrayList<Double> latencies = UtilFunctions.getValues(latencyRecords);
			ArrayList<Double> capacities = new ArrayList<>();
			Integer size = latencies.size();
			for(int i = 0; i < size; i++){
				capacities.add(1000 / latencies.get(i));//to consider capacity as a number of processed items per second
			}
			
			Double lastCapacity = 1000 / ComponentWindowedStats.getLastRecord(latencyRecords);
			Double stdDerivation = UtilFunctions.getStdDerivation(capacities);
			
			Double estimMaxCapacity = delta * (lastCapacity + stdDerivation);
			
			this.estimMaxCap.put(component, estimMaxCapacity);
		}
	}
	
	public void computeUtilCPU(ComponentMonitor cm, AssignmentMonitor am, TopologyExplorer explorer){
		Double alpha = 0.8;// to turn into config parameter
		Set<String> components = cm.getRegisteredComponents();
		HashMap<String, Double> cpuConstraints = cm.getCurrentCpuConstraints(explorer);
		for(String component : components){
			ArrayList<WorkerSlot> workers = am.getAllocatedWorkers(component);
			ArrayList<Double> utilCPUs = new ArrayList<>();
			for(WorkerSlot worker : workers){
				String host = worker.getNodeId();
				Integer port = worker.getPort();
				Double freeCPU = 100.0;
				Double allocatedCPU = 0.0;
				ArrayList<String> runningComponents = am.getRunningComponents(worker);
				for(String other : runningComponents){
					if(!other.equalsIgnoreCase(component)){
						allocatedCPU += cpuConstraints.get(other);
					}
				}
				
				Double requiredCPU = Math.max(ComponentWindowedStats.getLastRecord(cm.getCpuUsageOnWorker(component, host, port)), cpuConstraints.get(component));
				
				freeCPU -= allocatedCPU;
				freeCPU -= requiredCPU;
				
				Double utilCPU = alpha * (requiredCPU + (freeCPU / runningComponents.size()));
				utilCPUs.add(utilCPU / 100);// from percentage to ratio
			}
			this.utilCPU.put(component, UtilFunctions.getMinValue(utilCPUs));
		}
	}
	
	public void computeScalingActions(ComponentMonitor cm, AssignmentMonitor am, TopologyExplorer explorer){
		Double alpha = 0.8;// to turn into config parameter
		Integer delta = cm.getParser().getWindowSize();
		Set<String> components = cm.getRegisteredComponents();
		HashMap<String, Double> cpuConstraints = cm.getCurrentCpuConstraints(explorer);
		for(String component : components){
			if(needScaleOut(component)){
				Double estimInput = getEstimInput(component);
				
				Double allocCPU = cpuConstraints.get(component);
				
				HashMap<Integer, Double> latencyRecords = cm.getStats(component).getAvgLatencyRecords();
				Double lastLatency = ComponentWindowedStats.getLastRecord(latencyRecords);
				ArrayList<Double> latencies = UtilFunctions.getValues(latencyRecords);
				Double stdDerivation = UtilFunctions.getStdDerivation(latencies);
				Double latency = lastLatency + stdDerivation;
				
				Integer maxDegree = am.getAllSortedTasks(component).size();
				Integer currentDegree = getDegree(component);
				
				System.out.println("Component " + component + ": ");
				System.out.println("\t Estim inputs: " + estimInput);
				System.out.println("\t CPU constraint: " + allocCPU);
				System.out.println("\t Latency per tuple: " + latency);
				System.out.println("\t Current degree: " + currentDegree);
				System.out.println("\t Max degree: " + maxDegree);
				System.out.println("\t alpha: " + alpha);
				System.out.println("\t delta: " + delta);
				
				Integer kprime = Math.min(maxDegree, (int) Math.floor(estimInput / ((allocCPU / 100) * alpha * (1000 / latency) * delta)));
				
				Integer argmink = kprime - 1;
				while(validDegree(argmink, estimInput, allocCPU, latency, alpha, delta)){
					System.out.println("Reducing kprime from " + kprime + " to " + argmink);
					kprime = argmink;
					argmink--;
				}
				System.out.println("\t New degree: " + kprime);
				
				if(kprime > currentDegree){
					this.scaleOutActions.put(component, kprime);
				}
			}else{
				if(needScaleIn(component, cm.getParser())){
					Double estimInput = getEstimInput(component);
					
					Double allocCPU = cpuConstraints.get(component);
					
					HashMap<Integer, Double> latencyRecords = cm.getStats(component).getAvgLatencyRecords();
					Double lastLatency = ComponentWindowedStats.getLastRecord(latencyRecords);
					ArrayList<Double> latencies = UtilFunctions.getValues(latencyRecords);
					Double stdDerivation = UtilFunctions.getStdDerivation(latencies);
					Double latency = lastLatency + stdDerivation;
					
					Integer currentDegree = am.getParallelism(component);
					
					System.out.println("Component " + component + ": ");
					System.out.println("\t Estim inputs: " + estimInput);
					System.out.println("\t CPU constraint: " + allocCPU);
					System.out.println("\t Latency per tuple: " + latency);
					System.out.println("\t Current degree: " + currentDegree);
					System.out.println("\t alpha: " + alpha);
					System.out.println("\t delta: " + delta);
					
					Integer kprime = Math.max(1, (int) Math.floor(estimInput / ((allocCPU / 100) * alpha * (1000 / latency) * delta)));
					
					
					Integer argmink = kprime - 1;
					while(validDegree(argmink, estimInput, allocCPU, latency, alpha, delta)){
						System.out.println("Reducing kprime from " + kprime + " to " + argmink);
						kprime = argmink;
						argmink--;
					}
					System.out.println("\t New degree: " + kprime);
					if(kprime < currentDegree){
						this.scaleInActions.put(component, kprime);
					}
				}
			}
		}
	}
	
	public boolean validDegree(Integer degree, Double estimInput, Double allocCPU, Double latency, Double alpha, Integer delta){
		Double adequation = estimInput / (degree * Math.ceil((allocCPU / 100) * alpha * (1000 / latency) * delta));
		return adequation < 1;
	}
	
	public boolean needScaleOut(String component){
		Integer k = getDegree(component);
		Double estimGlobalCapacity = k * Math.ceil(getEstimMaxCapacity(component) * getUtilCPU(component));
		return (getEstimInput(component) / estimGlobalCapacity) >= 1;
	}
	
	public boolean needScaleIn(String component, XmlConfigParser parser){
		Integer k = getDegree(component);
		Double thetaMin = parser.getLowActivityThreshold();
		Double estimGlobalCapacity = k * Math.ceil(getEstimMaxCapacity(component) * getUtilCPU(component));
		return (getEstimInput(component) / estimGlobalCapacity) < thetaMin;
	}
	
	public Double combine(Double estim1, Double estim2){
		return Math.max(estim1, estim2); //combine strategy can be replaced by min, avg...
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
	
	public HashMap<String, Integer> getScaleInActions(){
		return this.scaleInActions;
	}
	
	public HashMap<String, Integer> getScaleOutActions(){
		return this.scaleOutActions;
	}
	
	public Integer getDegree(String component){
		return this.degrees.get(component);
	}
}