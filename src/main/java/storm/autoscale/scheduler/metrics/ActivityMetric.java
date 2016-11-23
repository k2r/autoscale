/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.math.BigDecimal;
import java.util.HashMap;

import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.regression.LinearRegressionTools;

/**
 * @author Roland
 *
 */
public class ActivityMetric implements IMetric {

	ComponentMonitor cm;
	AssignmentMonitor am;
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
	public ActivityMetric(ComponentMonitor cm, AssignmentMonitor am, TopologyExplorer explorer) {
		this.cm = cm;
		this.am = am;
		this.explorer = explorer;
		this.remainingTuples = this.cm.getPendingTuples(this.explorer);
		this.activityInfo = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#getComponentMonitor()
	 */
	@Override
	public ComponentMonitor getComponentMonitor() {
		return this.cm;
	}

	public HashMap<String, BigDecimal> getActivityInfo(String component){
		return this.activityInfo.get(component);
	}
	
	public Double computeEstimatedLoad(String component){
		Double result = 0.0;
		//Get the remaining tuples for the component
		Long remainingTuples = this.cm.getStats(component).getTotalInput() - this.cm.getStats(component).getTotalExecuted();
		if(!this.activityInfo.containsKey(component)){
			this.activityInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.activityInfo.get(component);
		info.put(REMAINING, new BigDecimal(remainingTuples));
		//Determine the min and the max of input records
		HashMap<Integer, Long> inputRecords = this.cm.getStats(component).getInputRecords();
		//From those points, compute the equation of the line
		Double coeff = LinearRegressionTools.linearRegressionCoeff(inputRecords);
		Double offset = LinearRegressionTools.linearRegressionOffset(inputRecords);
		//determine each estimated value and sum them
		Integer lastTimestamp = ComponentWindowedStats.getRecordedTimestamps(inputRecords).get(0);
		Integer nextWindowEnd = lastTimestamp + this.cm.getParser().getWindowSize();
		Integer monitoringFrequency = this.cm.getMonitoringFrequency();
		Double estimIncomingTuples = 0.0;
		for(int i = lastTimestamp + monitoringFrequency; i <= nextWindowEnd; i += monitoringFrequency){
			Double estimation = Math.max(0, coeff * i + offset);
			estimIncomingTuples += estimation;
		}
		info.put(ESTIMLOAD, new BigDecimal(estimIncomingTuples));
		//sum the remaining tuples at the end of the current window to the estimated value of incoming tuples into result variable
		result = remainingTuples + estimIncomingTuples;
		return result;
	}
	
	public Double computeAvgCapacity(String component){
		Double result = 0.0;
		//Get the window size
		Integer windowSize = this.cm.getParser().getWindowSize();
		HashMap<Integer, Double> latencyRecords = this.cm.getStats(component).getAvgLatencyRecords();
		HashMap<Integer, Double> processingRates = new HashMap<>();
		for(Integer timestamp : latencyRecords.keySet()){
			processingRates.put(timestamp, 1000 / latencyRecords.get(timestamp));
		}
		Double averageSeqCapacity = LinearRegressionTools.avgYCoordinate(processingRates);
		Integer parallelism = this.am.getParallelism(component); 
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
		Integer nbRecords = ComponentWindowedStats.getRecordedTimestamps(this.cm.getStats(component).getInputRecords()).size();
		Double epr = this.computeEstimatedLoad(component) / this.computeAvgCapacity(component);
		//In the case, we estimate that no tuples will be processed, we affect a special value to let a grace period 
		if(epr.isInfinite() || epr.isNaN() || nbRecords < 4){
			epr = -1.0;
		}
		if(!this.activityInfo.containsKey(component)){
			this.activityInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.activityInfo.get(component);
		info.put(ACTIVITY, new BigDecimal(epr));
		return epr;
	}
}