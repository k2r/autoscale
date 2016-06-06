/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class ComponentMonitor {
	
	private HashMap<String, ComponentStats> stats;
	private StatStorageManager manager;
	private static Logger logger = Logger.getLogger("ComponentMonitor");
	
	/**
	 * 
	 */
	public ComponentMonitor(String dbHost, String nimbusHost, Integer nimbusPort, Integer rate) {
		this.stats = new HashMap<>();
		try {
			this.manager = StatStorageManager.getManager(dbHost, nimbusHost, nimbusPort, rate);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void getStatistics(TopologyExplorer explorer){
		Integer current = this.manager.getCurrentTimestamp();
		Integer previous = -1;
		if(current > 0){
			previous = current - 1;
		} 
		Set<String> spouts = explorer.getSpouts();
		for(String spout : spouts){
			Double nbInputs = 0.0;
			Double nbExecuted = 0.0;
			Double nbOutputs = this.manager.getOutputs(spout, current)  - this.manager.getOutputs(spout, previous) * 1.0;
			Double avgLatency = this.manager.getAvgLatency(spout, current) * 1.0;
			ComponentStats component = new ComponentStats(spout, nbInputs, nbExecuted, nbOutputs, avgLatency);
			this.stats.put(spout, component);
		}
		
		Set<String> bolts = explorer.getSpouts();
		for(String bolt : bolts){
			Double nbInputs = 0.0;
			ArrayList<String> parents = explorer.getParents(bolt);
			for(String parent : parents){
				Long parentOutput = this.manager.getOutputs(parent, current) - this.manager.getOutputs(parent, previous); 
				nbInputs += parentOutput;
			}
			Double nbExecuted = this.manager.getExecuted(bolt, current) - this.manager.getExecuted(bolt, previous) * 1.0;
			Double nbOutputs = this.manager.getOutputs(bolt, current)  - this.manager.getOutputs(bolt, previous) * 1.0;
			Double avgLatency = this.manager.getAvgLatency(bolt, current) * 1.0;
			ComponentStats component = new ComponentStats(bolt, nbInputs, nbExecuted, nbOutputs, avgLatency);
			this.stats.put(bolt, component);
		}
	}
	
	public ComponentStats getStats(String component){
		return this.stats.get(component);
	}
	
	public void updateStatistics(ComponentStats stats){
		if(this.stats.containsKey(stats)){
			this.stats.replace(stats.getId(), stats);
		}else{
			this.stats.put(stats.getId(), stats);
		}
	}
	
	public boolean isCongested(String component){
		if(this.stats.containsKey(component)){
			ComponentStats componentStats = this.stats.get(component);
			if(componentStats.getNbInputs() > componentStats.getNbExecuted()){
				return true;
			}else{
				return false;
			}
		}else{
			logger.warning("Looking for an non-existing component " + component);
			return false;
		}
	}
	
	public boolean couldCongest(String component){
		//TODO Anticipate a congestion thanks to the condition: CPU usage of supervisor > threshold & inputs growing up
		return false;
	}
	
	public ArrayList<String> getCongested(){
		ArrayList<String> result = new ArrayList<>();
		for(String component : this.stats.keySet()){
			if(isCongested(component)){
				result.add(component);
			}
		}
		return result;
	}
	
	public ArrayList<String> getPotentialCongested(){
		ArrayList<String> result = new ArrayList<>();
		for(String component : this.stats.keySet()){
			if(couldCongest(component)){
				result.add(component);
			}
		}
		return result;
	}
}