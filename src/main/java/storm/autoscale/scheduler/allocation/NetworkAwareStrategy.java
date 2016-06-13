/**
 * 
 */
package storm.autoscale.scheduler.allocation;

import java.util.ArrayList;
import java.util.HashMap;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentStats;

/**
 * @author Roland
 *
 */
public class NetworkAwareStrategy implements IAllocationStrategy {

	private TopologyExplorer explorer;
	private ComponentMonitor compMonitor;
	private AssignmentMonitor assignMonitor;
	
	public NetworkAwareStrategy(TopologyExplorer explorer, ComponentMonitor cm, AssignmentMonitor am) {
		this.explorer = explorer;
		this.compMonitor = cm;
		this.assignMonitor = am;
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.allocation.IAllocationStrategy#computeScore(java.lang.String, backtype.storm.scheduler.WorkerSlot)
	 */
	@Override
	public Double computeScore(String component, WorkerSlot worker) {
		Double result = 0.0;
		HashMap<SupervisorDetails, ArrayList<WorkerSlot>> supToWorkers = this.assignMonitor.getWorkers();
		SupervisorDetails supervisor = this.assignMonitor.getSupervisor(worker, supToWorkers);
		ArrayList<WorkerSlot> neighbours = supToWorkers.get(supervisor);
		neighbours.remove(worker);//we do not need to count the candidate in his neighborhood
		for(WorkerSlot neighbour : neighbours){
			ArrayList<String> neighbourComponents = this.assignMonitor.getRunningComponents(neighbour);
			for(String neighbourComp : neighbourComponents){
				if(this.explorer.areLinked(component, neighbourComp)){
					//check which one is the parent and which is the child, add the volume of the stream linking them
				}
			}
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.allocation.IAllocationStrategy#getScores(java.lang.String)
	 */
	@Override
	public HashMap<WorkerSlot, Double> getScores(String component) {
		// TODO Auto-generated method stub
		return null;
	}

}
