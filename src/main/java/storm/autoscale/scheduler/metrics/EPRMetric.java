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
		//compute the remaining tuples at the end of the current window (remaining from the last one + total input - total executed)
		Long currentRemainingTuples = formerRemaining + this.cm.getStats(component).getTotalInput() - this.cm.getStats(component).getTotalExecuted();
		if(!this.eprInfo.containsKey(component)){
			this.eprInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.eprInfo.get(component);
		info.put(REMAINING, new BigDecimal(currentRemainingTuples));
		//Determine the min and the max of input records
		Integer minTimestamp = 0;
		Long minInput = 0L;
		Integer maxTimestamp = 0;
		Long maxInput = 0L;
		HashMap<Integer, Long> inputRecords = this.cm.getStats(component).getInputRecords();
		for(Integer timestamp : inputRecords.keySet()){
			Long record = inputRecords.get(timestamp);
			if(record < minInput){
				minTimestamp = timestamp;
				minInput = record;
			}else{
				if(record > maxInput){
					maxTimestamp = timestamp;
					maxInput = record;
				}
			}
		}
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
			}
		}
		//determine each estimated value and sum them
		Integer lastTimestamp = ComponentWindowedStats.getRecordedTimestamps(inputRecords).get(0);
		Integer nextWindowEnd = lastTimestamp + ComponentMonitor.WINDOW_SIZE;
		Integer samplingRate = this.cm.getSamplingRate();
		Double estimIncomingTuples = 0.0;
		for(int i = lastTimestamp + samplingRate; i <= nextWindowEnd; i += samplingRate){
			estimIncomingTuples += coeff * i + offset;
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
		Double minProcRate = 0.0;
		Integer maxTimestamp = 0;
		Double maxProcRate = 0.0;
		HashMap<Integer, Double> latencyRecords = this.cm.getStats(component).getAvgLatencyRecords();
		for(Integer timestamp : latencyRecords.keySet()){
			Double record = 0.0;
			try{
				record = 1 / latencyRecords.get(timestamp);
			} catch(ArithmeticException e) {
			}
			if(record < minProcRate){
				minTimestamp = timestamp;
				minProcRate = record;
			}else{
				if(record > maxProcRate){
					maxTimestamp = timestamp;
					maxProcRate = record;
				}
			}
		}
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
			}
		}
		//determine each estimated value and calculate the average
		Integer lastTimestamp = ComponentWindowedStats.getRecordedTimestamps(latencyRecords).get(0);
		Integer nextWindowEnd = lastTimestamp + windowSize;
		Integer samplingRate = this.cm.getSamplingRate();
		Double estimSumProcRate = 0.0;
		Double count = 0.0;
		for(int i = lastTimestamp + samplingRate; i <= nextWindowEnd; i += samplingRate){
			estimSumProcRate += coeff * i + offset;
			count++;
		}
		Double estimAvgProcRate = estimSumProcRate / count;
		result = windowSize * estimAvgProcRate;
		
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
		if(!this.eprInfo.containsKey(component)){
			this.eprInfo.put(component, new HashMap<String, BigDecimal>());
		}
		HashMap<String, BigDecimal> info = this.eprInfo.get(component);
		info.put(EPR, new BigDecimal(epr));
		return epr;
	}

}