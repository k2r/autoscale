/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.math.BigDecimal;
import java.util.HashMap;

import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;

/**
 * @author Roland
 *
 */
public class LoadMetric implements IMetric {

	ComponentMonitor cm;
	AssignmentMonitor am;
	HashMap<String, HashMap<String, BigDecimal>> loadInfo;
	public static final String PR = "processing_ratio";
	public static final String LOAD = "current_load";
	
	
	public LoadMetric(ComponentMonitor cm, AssignmentMonitor am) {
		this.cm = cm;
		this.am = am;
		this.loadInfo = new HashMap<>();
	}


	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#getComponentMonitor()
	 */
	@Override
	public ComponentMonitor getComponentMonitor() {
		return cm;
	}
	
	public HashMap<String, BigDecimal> getLoadInfo(String component){
		return this.loadInfo.get(component);
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#compute(java.lang.String)
	 */
	@Override
	public Double compute(String component) {
		HashMap<String, BigDecimal> info = new HashMap<>();
		Integer parallelism = this.am.getParallelism(component);
		Integer windowSize = ComponentMonitor.WINDOW_SIZE;
		
		HashMap<Integer, Double> latencyRecords = this.cm.getStats(component).getAvgLatencyRecords();
		Double sum = 0.0;
		Double count = 0.0;
	
		for(Integer timestamp : latencyRecords.keySet()){
			Double processingRate =  1000 / latencyRecords.get(timestamp);
			sum += processingRate;
			count++;
		}
		Double avgProcessingCapacity = (sum / count) * parallelism * windowSize;
		
		Double processingLoad = 0.0;
		Double incomingLoad = 0.0;
		
		if(!avgProcessingCapacity.isInfinite() && !avgProcessingCapacity.isNaN()){
			Long executed = this.cm.getStats(component).getTotalExecuted();
			processingLoad = 1 - ((avgProcessingCapacity - executed) / avgProcessingCapacity);
			
			Long input = this.cm.getStats(component).getTotalInput();
			incomingLoad = 1 - ((avgProcessingCapacity - input) / avgProcessingCapacity);
		}
		info.put(PR, new BigDecimal(processingLoad));
		info.put(LOAD, new BigDecimal(incomingLoad));
		this.loadInfo.put(component, info);
		return processingLoad;	
	}
}