/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.util.ArrayList;
import java.util.HashMap;

import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

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
	
	/**
	 * @return the impactDegrees of registered components
	 */
	public HashMap<String, Integer> getImpactDegrees() {
		return impactDegrees;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#compute(java.lang.String)
	 */
	@Override
	public Double compute(String component) {
		Double result = 0.0;// we return the value Impact(*,component)
		ArrayList<String> parents = this.explorer.getParents(component);
		for(String parent : parents){
			Double estimatedLoad = this.cm.getEstimatedLoad(parent);
			Double selectivity = ComponentWindowedStats.getLastRecord(this.cm.getStats(parent).getSelectivityRecords());
			Double impact = estimatedLoad * selectivity;
			result += impact;
		}
		
		Double capacityPerWindow = this.cm.getCapacity(component) * this.cm.getParser().getWindowSize();
		int impactDegree = (int) Math.round(result / capacityPerWindow);
		this.impactDegrees.put(component, impactDegree);
		return result;
	}
}