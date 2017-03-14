/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.math.BigDecimal;
import java.util.HashMap;

import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.ScalingManager;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.regression.LinearRegressionTools;
import storm.autoscale.scheduler.regression.RegressionSelector;

/**
 * @author Roland
 *
 */
public class ActivityMetric implements IMetric {

	ScalingManager sm;
	TopologyExplorer explorer;
	HashMap<String, Long> remainingTuples;
	HashMap<String, HashMap<String, BigDecimal>> activityInfo;
	
	public static final String REMAINING = "remaining_tuples";
	public static final String ACTIVITY = "activity_level";
	public static final String CAPPERSEC = "capacity_per_second";
	public static final String ESTIMLOAD = "estimated_load";
	
	/**
	 * 
	 */
	public ActivityMetric(ScalingManager sm, TopologyExplorer explorer) {
		this.sm = sm;
		this.explorer = explorer;
		this.remainingTuples = this.sm.getMonitor().getPendingTuples(this.explorer);
		this.activityInfo = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#getComponentMonitor()
	 */
	@Override
	public ComponentMonitor getComponentMonitor() {
		return this.sm.getMonitor();
	}
	
	@Override
	public TopologyExplorer getTopologyExplorer() {
		return this.explorer;
	}

	public HashMap<String, BigDecimal> getActivityInfo(String component){
		return this.activityInfo.get(component);
	}
	
	public Double computeEstimatedLoad(String component){
		Double result = 0.0;
		ComponentMonitor cm = this.getComponentMonitor();
		//Get the remaining tuples for the component
		Long remainingTuples = cm.getStats(component).getTotalInput() - cm.getStats(component).getTotalExecuted();
		if(!this.activityInfo.containsKey(component)){
			this.activityInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.activityInfo.get(component);
		info.put(REMAINING, new BigDecimal(remainingTuples));
		//Determine the min and the max of input records
		HashMap<Integer, Long> inputRecords = cm.getStats(component).getInputRecords();
		//From those points, compute the equation of the line
		RegressionSelector<Integer, Long> regression = new RegressionSelector<>(inputRecords);
		
		//determine each estimated value and sum them
		Integer lastTimestamp = ComponentWindowedStats.getRecordedTimestamps(inputRecords).get(0);
		Integer nextWindowEnd = lastTimestamp + cm.getParser().getWindowSize();
		Integer monitoringFrequency = cm.getMonitoringFrequency();
		Double estimIncomingTuples = 0.0;
		for(int i = lastTimestamp + monitoringFrequency; i <= nextWindowEnd; i += monitoringFrequency){
			Double estimation = Math.max(0, regression.estimateYCoordinate(i));
			estimIncomingTuples += estimation;
		}
		if(estimIncomingTuples.isInfinite() || estimIncomingTuples.isNaN()){
			estimIncomingTuples = 0.0;
		}
		info.put(ESTIMLOAD, new BigDecimal(estimIncomingTuples));
		//sum the remaining tuples at the end of the current window to the estimated value of incoming tuples into result variable
		result = remainingTuples + estimIncomingTuples;
		return result;
	}
	
	public Double computeAvgCapacity(String component){
		Double result = 0.0;
		ComponentMonitor cm = this.getComponentMonitor();
		//Get the window size
		Integer windowSize = cm.getParser().getWindowSize();
		HashMap<Integer, Double> latencyRecords = cm.getStats(component).getAvgLatencyRecords();
		HashMap<Integer, Double> processingRates = new HashMap<>();
		for(Integer timestamp : latencyRecords.keySet()){
			processingRates.put(timestamp, 1000 / latencyRecords.get(timestamp));
		}
		Double averageSeqCapacity = LinearRegressionTools.avgYCoordinate(processingRates);
		Integer parallelism = this.sm.getDegree(component); 
		Double avgCapacity = averageSeqCapacity * parallelism;
		if(avgCapacity.isInfinite() || avgCapacity.isNaN()){
			avgCapacity = 0.0;
		}
		result = windowSize * avgCapacity;
		if(!this.activityInfo.containsKey(component)){
			this.activityInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.activityInfo.get(component);
		info.put(CAPPERSEC, new BigDecimal(avgCapacity));
		//multiply these average with the window size into result variable
		return result;
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#compute(java.lang.String)
	 */
	@Override
	public Double compute(String component) {
		ComponentMonitor cm = this.sm.getMonitor();
		Integer nbRecords = ComponentWindowedStats.getRecordedTimestamps(cm.getStats(component).getInputRecords()).size();
		Double activity = this.computeEstimatedLoad(component) / this.computeAvgCapacity(component);
		Integer expectedNbRecords = (this.sm.getParser().getWindowSize() / cm.getMonitoringFrequency()) - 1;// we accept an offset of one measurement compared to ideal model 
		//In the case, we estimate that no tuples will be processed, we affect a special value to let a grace period 
		if(activity.isInfinite() || activity.isNaN() || nbRecords < expectedNbRecords){
			activity = -1.0;
		}
		if(!this.activityInfo.containsKey(component)){
			this.activityInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.activityInfo.get(component);
		info.put(ACTIVITY, new BigDecimal(activity));
		return activity;
	}
}