/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.util.HashMap;

import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class ImpactMetric implements IMetric {

	ComponentMonitor cm;
	TopologyExplorer explorer;
	HashMap<String, Integer> impactDegrees;
	
	public ImpactMetric(ComponentMonitor compMonitor, TopologyExplorer explorer) {
		this.cm = compMonitor;
		this.explorer = explorer;
		this.impactDegrees = new HashMap<>();
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#getComponentMonitor()
	 */
	@Override
	public ComponentMonitor getComponentMonitor() {
		return this.cm;
	}
	
	@Override
	public TopologyExplorer getTopologyExplorer() {
		return this.explorer;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#compute(java.lang.String)
	 */
	@Override
	public Double compute(String component) {
		// TODO Auto-generated method stub
		return null;
	}
}
