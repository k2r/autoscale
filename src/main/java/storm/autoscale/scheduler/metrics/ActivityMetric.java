/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.math.BigDecimal;
import java.util.HashMap;

import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.util.RegressionTools;

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
	
	/**
	 * 
	 */
	public ActivityMetric(ComponentMonitor cm, AssignmentMonitor am, TopologyExplorer explorer) {
		this.cm = cm;
		this.am = am;
		this.explorer = explorer;
		this.remainingTuples = this.cm.getFormerRemainingTuples(this.explorer);
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
		//System.out.println("Tuples remaining at the start of the window : " + formerRemaining);
		//System.out.println("Tuples remaining now: " + currentRemainingTuples);
		if(!this.activityInfo.containsKey(component)){
			this.activityInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.activityInfo.get(component);
		info.put(REMAINING, new BigDecimal(remainingTuples));
		//Determine the min and the max of input records
		HashMap<Integer, Long> inputRecords = this.cm.getStats(component).getInputRecords();
		//From those points, compute the equation of the line
		Double coeff = RegressionTools.linearRegressionCoeff(inputRecords);
		Double offset = RegressionTools.linearRegressionOffset(inputRecords);
		//System.out.println("Linear coefficient : " + coeff);
		//System.out.println("Linear offset : " + offset);
		//determine each estimated value and sum them
		Integer lastTimestamp = ComponentWindowedStats.getRecordedTimestamps(inputRecords).get(0);
		Integer nextWindowEnd = lastTimestamp + ComponentMonitor.WINDOW_SIZE;
		Integer samplingRate = this.cm.getSamplingRate();
		Double estimIncomingTuples = 0.0;
		//System.out.println("Estimation : ");
		for(int i = lastTimestamp + samplingRate; i <= nextWindowEnd; i += samplingRate){
			Double estimation = Math.max(0, coeff * i + offset);
			estimIncomingTuples += estimation;
			//System.out.println("timestamp : " + i  + " estimation : " + estimation);
		}
		//sum the remaining tuples at the end of the current window to the estimated value of incoming tuples into result variable
		result = remainingTuples + estimIncomingTuples;
		return result;
	}
	
	public Double computeAvgCapacity(String component){
		Double result = 0.0;
		//Get the window size
		Integer windowSize = ComponentMonitor.WINDOW_SIZE;
		HashMap<Integer, Double> latencyRecords = this.cm.getStats(component).getAvgLatencyRecords();
		HashMap<Integer, Double> processingRates = new HashMap<>();
		for(Integer timestamp : latencyRecords.keySet()){
			processingRates.put(timestamp, 1000 / latencyRecords.get(timestamp));
		}
		Double averageSeqCapacity = RegressionTools.avgYCoordinate(processingRates);
		//Double coeff = RegressionTools.linearRegressionCoeff(processingRates);
		//Double offset = RegressionTools.linearRegressionOffset(processingRates);
		//System.out.println("Linear coefficient : " + coeff);
		//System.out.println("Linear offset : " + offset);
		//determine each estimated value and calculate the average
		//Integer lastTimestamp = ComponentWindowedStats.getRecordedTimestamps(latencyRecords).get(0);
		//Integer nextWindowEnd = lastTimestamp + windowSize;
		/*Integer samplingRate = this.cm.getSamplingRate();
		Double estimSumProcRate = 0.0;
		Double count = 0.0;
		for(int i = lastTimestamp + samplingRate; i <= nextWindowEnd; i += samplingRate){
			Double estimProcRate = Math.max(0, coeff * i + offset);
			estimSumProcRate += estimProcRate;
			count++;
			//System.out.println("timstamp : " + i + " estimated number of processed tuples : " + estimProcRate);
		}*/
		Integer parallelism = this.am.getParallelism(component); 
		Double avgCapacity = averageSeqCapacity * parallelism;
		if(avgCapacity.isInfinite() || avgCapacity.isNaN()){
			avgCapacity = 0.0;
		}
		//System.out.println("Estimated processing rate : " + estimAvgProcRate);
		result = windowSize * avgCapacity;
		//System.out.println("Estimated number of processed tuples on the next window: " + result);
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