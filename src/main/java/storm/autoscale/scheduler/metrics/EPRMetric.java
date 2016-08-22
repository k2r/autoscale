/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.math.BigDecimal;
import java.util.HashMap;

import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class EPRMetric implements IMetric {

	TopologyExplorer te;
	ComponentMonitor cm;
	HashMap<String, Long> remainingTuples;
	HashMap<String, HashMap<String, BigDecimal>> eprInfo;
	public static final String REMAINING = "remaining_tuples";
	public static final String EPR = "epr";
	public static final String PROCRATE = "processing_rate";
	
	/**
	 * 
	 */
	public EPRMetric(TopologyExplorer te, ComponentMonitor cm) {
		this.te = te;
		this.cm = cm;
		this.remainingTuples = this.cm.getFormerRemainingTuples();
		this.eprInfo = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#getTopologyExplorer()
	 */
	@Override
	public TopologyExplorer getTopologyExplorer() {
		return this.te;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#getComponentMonitor()
	 */
	@Override
	public ComponentMonitor getComponentMonitor() {
		return this.cm;
	}

	public HashMap<String, BigDecimal> getEPRInfo(String component){
		return this.eprInfo.get(component);
	}
	
	public Double computeEstimatedLoad(String component){
		Double result = 0.0;
		//Get the remaining tuples for the component
		Long formerRemaining = this.remainingTuples.get(component);
		//System.out.println("Tuples remaining at the start of the window : " + formerRemaining);
		//compute the remaining tuples at the end of the current window (remaining from the last one + total input - total executed)
		Long currentRemainingTuples = formerRemaining + this.cm.getStats(component).getTotalInput() - this.cm.getStats(component).getTotalExecuted();
		//System.out.println("Tuples remaining now: " + currentRemainingTuples);
		if(!this.eprInfo.containsKey(component)){
			this.eprInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.eprInfo.get(component);
		info.put(REMAINING, new BigDecimal(currentRemainingTuples));
		//Determine the min and the max of input records
		Integer minTimestamp = 0;
		Long minInput = Long.MAX_VALUE;
		Integer maxTimestamp = 0;
		Long maxInput = Long.MIN_VALUE;
		HashMap<Integer, Long> inputRecords = this.cm.getStats(component).getInputRecords();
		for(Integer timestamp : inputRecords.keySet()){
			Long record = inputRecords.get(timestamp);
			if(record < minInput){
				minTimestamp = timestamp;
				minInput = record;
			}
			if(record > maxInput){
				maxTimestamp = timestamp;
				maxInput = record;
			}
		}
		//System.out.println("Min timestamp : " + minTimestamp);
		//System.out.println("Min input : " + minInput);
		//System.out.println("Max timestamp : " + maxTimestamp);
		//System.out.println("Max input : " + maxInput);
		//From those points, compute the equation of the line
		Double coeff = 0.0;
		Long offset = 0L;
		if(minTimestamp < maxTimestamp){
			coeff = (maxInput - minInput) / (1.0 * (maxTimestamp - minTimestamp));
			offset = minInput;
		}else{
			if(minTimestamp > maxTimestamp){
				coeff = (minInput - maxInput) / (1.0 * (minTimestamp - maxTimestamp));
				offset = maxInput;
			}else{
				offset = maxInput;
			}
		}
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
		result = currentRemainingTuples + estimIncomingTuples;
		return result;
	}
	
	public Double computeEstimatedProcessing(String component){
		Double result = 0.0;
		//Get the window size
		Integer windowSize = ComponentMonitor.WINDOW_SIZE;
		//Get the min and the max of the inverse of latency records
		Integer minTimestamp = 0;
		Double minProcRate = Double.MAX_VALUE;
		Integer maxTimestamp = 0;
		Double maxProcRate = Double.MIN_VALUE;
		HashMap<Integer, Double> latencyRecords = this.cm.getStats(component).getAvgLatencyRecords();
		for(Integer timestamp : latencyRecords.keySet()){
			Double record = 0.0;
			try{
				record = 1000 / latencyRecords.get(timestamp);
			} catch(ArithmeticException e) {
			}
			if(record < minProcRate){
				minTimestamp = timestamp;
				minProcRate = record;
			}
			if(record > maxProcRate){
				maxTimestamp = timestamp;
				maxProcRate = record;
			}
		}
		//System.out.println("Min timestamp : " + minTimestamp);
		//System.out.println("Min processing rate : " + minProcRate);
		//System.out.println("Max timestamp : " + maxTimestamp);
		//System.out.println("Max processing rate : " + maxProcRate);
		//From those points compute the equation of the line
		Double coeff = 0.0;
		Double offset = 0.0;
		if(minTimestamp < maxTimestamp){
			coeff = (maxProcRate - minProcRate) / (maxTimestamp - minTimestamp);
			offset = minProcRate;
		}else{
			if(minTimestamp > maxTimestamp){
				coeff = (minProcRate - maxProcRate) / (minTimestamp - maxTimestamp);
				offset = maxProcRate;
			}else{
				offset = maxProcRate;
			}
		}
		//System.out.println("Linear coefficient : " + coeff);
		//System.out.println("Linear offset : " + offset);
		//determine each estimated value and calculate the average
		Integer lastTimestamp = ComponentWindowedStats.getRecordedTimestamps(latencyRecords).get(0);
		Integer nextWindowEnd = lastTimestamp + windowSize;
		Integer samplingRate = this.cm.getSamplingRate();
		Double estimSumProcRate = 0.0;
		Double count = 0.0;
		for(int i = lastTimestamp + samplingRate; i <= nextWindowEnd; i += samplingRate){
			Double estimProcRate = Math.max(0, coeff * i + offset);
			estimSumProcRate += estimProcRate;
			count++;
			//System.out.println("timstamp : " + i + " estimated number of processed tuples : " + estimProcRate);
		}
		Double estimAvgProcRate = estimSumProcRate / count;
		//System.out.println("Estimated processing rate : " + estimAvgProcRate);
		result = windowSize * estimAvgProcRate;
		//System.out.println("Estimated number of processed tuples on the next window: " + result);
		if(!this.eprInfo.containsKey(component)){
			this.eprInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.eprInfo.get(component);
		info.put(PROCRATE, new BigDecimal(estimAvgProcRate));
		//multiply these average with the window size into result variable
		return result;
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#compute(java.lang.String)
	 */
	@Override
	public Double compute(String component) {
		Double epr = this.computeEstimatedLoad(component) / this.computeEstimatedProcessing(component);
		//In the case, we estimate that no tuples will be processed, we affect a special value to let a grace period 
		if(epr.isInfinite() || epr.isNaN()){
			epr = -1.0;
		}
		if(!this.eprInfo.containsKey(component)){
			this.eprInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.eprInfo.get(component);
		info.put(EPR, new BigDecimal(epr));
		return epr;
	}

}