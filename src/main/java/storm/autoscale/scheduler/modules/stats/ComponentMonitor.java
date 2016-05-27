/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class ComponentMonitor {
	
	private HashMap<String, ComponentStats> stats;
	private StatStorageManager manager;
	
	/**
	 * 
	 */
	public ComponentMonitor(String dbHost, String nimbusHost, Integer nimbusPort) {
		this.stats = new HashMap<>();
		try {
			this.manager = StatStorageManager.getManager(dbHost, nimbusHost, nimbusPort);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void getStatistics(TopologyExplorer explorer){
		//TODO Fullfil the map with all mandatory attributes thanks to previous and current stats provided by the manager 
	}
	
	public boolean isCongested(String component){
		//TODO Check the condition for congestion, first step inputs > executed
		return false;
	}
	
	public boolean couldCongest(String component){
		//TODO Anticipate a congestion thanks to the condition: CPU usage of supervisor > threshold & inputs growing up
		return false;
	}
	
	public ArrayList<String> getCongested(){
		ArrayList<String> result = new ArrayList<>();
		//TODO all congested components
		return result;
	}
	
	public ArrayList<String> getPotentialCongested(){
		ArrayList<String> result = new ArrayList<>();
		//TODO all potential congested components 
		return result;
	}
}