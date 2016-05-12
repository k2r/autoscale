/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.util.ArrayList;

import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class WelfMetric implements IMetric {

	TopologyExplorer te;
	ComponentMonitor cm;
	
	/**
	 * 
	 */
	public WelfMetric(TopologyExplorer te, ComponentMonitor cm) {
		this.te = te;
		this.cm = cm;
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

	public Double estimatedLatency(String component){
		Double inputs = this.cm.getInputQueueSize(component);
		Double selectivity = this.cm.getEstimatedSelectivy(component);
		Double latency = this.cm.getAvgLatency(component);
		if(latency == null){
			latency = 1.0; //ONLY FOR DEBUGGING PURPOSE
		}
		return inputs * selectivity * latency;
	}
	
	public Double computePathLatency(String component){
		Double pathLatency = 0.0;
		Double componentLatency = this.estimatedLatency(component);
		pathLatency += componentLatency;
		ArrayList<String> children = this.te.getChildren(component);
		if(!children.isEmpty()){
			for(String child : children){
				if(!this.cm.isCongested(child)){
					pathLatency += computePathLatency(child);
				}
			}
		}
		return pathLatency;
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#compute(java.lang.String)
	 */
	@Override
	public Double compute(String component) {
		Double pathLatency = this.computePathLatency(component);
		Double componentLatency = this.estimatedLatency(component);
		return componentLatency / pathLatency;
	}
}