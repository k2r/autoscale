/**
 * 
 */
package storm.autoscale.scheduler.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;

import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

import storm.autoscale.scheduler.allocation.IAllocationStrategy;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class ScaleOutAction implements IAction {

	private ComponentWindowedStats stats;
	private TopologyDetails topology;
	private AssignmentMonitor assignMonitor;
	private IAllocationStrategy allocStrategy;
	private Logger logger = Logger.getLogger("ScaleOutAction");
	
	public ScaleOutAction(ComponentWindowedStats stats, TopologyDetails topology, AssignmentMonitor am, IAllocationStrategy as) {
		this.stats = stats;
		this.topology = topology;
		this.assignMonitor = am;
		this.allocStrategy = as;
		Thread thread = new Thread(this);
		thread.start();
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.actions.IAction#getBestLocation()
	 */
	@Override
	public WorkerSlot getBestLocation() {
		WorkerSlot result = null;
		Double bestScore = Double.NEGATIVE_INFINITY;
		HashMap<WorkerSlot, Double> scores = this.allocStrategy.getScores(this.stats.getId());
		for(WorkerSlot ws : scores.keySet()){
			Double score = scores.get(ws);
			if(score > bestScore){
				result = ws;
				bestScore = score;
			}
		}
		return result;
	}

	@Override
	public void run() {
		String component = this.stats.getId();
		Double inputs = ComponentWindowedStats.getLastRecord(this.stats.getInputRecords());
		Double executed = ComponentWindowedStats.getLastRecord(this.stats.getExecutedRecords());
		int nbExecToAdd = (int) Math.round((inputs - executed) / executed); 
		ArrayList<Integer> tasks = this.assignMonitor.getAllSortedTasks(component);
		int currentParallelism = this.assignMonitor.getParallelism(component);
		int newParallelism = Math.min(tasks.size(), currentParallelism + nbExecToAdd);
		
		if(newParallelism > currentParallelism){
			RebalanceOptions options = new RebalanceOptions();
			options.put_to_num_executors(component, newParallelism);
			options.set_num_workers(this.assignMonitor.getNbWorkers());
			options.set_wait_secs(1);

			TSocket tsocket = new TSocket("localhost", 6627);
			TFramedTransport tTransport = new TFramedTransport(tsocket);
			TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
			Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);

			logger.info("Changing parallelism degree of component " + component + " from " + currentParallelism + " to " + newParallelism + "...");
			try {
				if(!tTransport.isOpen()){
					tTransport.open();
				}
				client.rebalance(topology.getName(), options);
				logger.info("Parallelism of component " + component + " increased successfully!");
				tTransport.close();
			} catch (TException e) {
				logger.severe("Unable to scale topology " + topology.getName() + " because of " + e);
			}
		}else{
			logger.info("This scale-out will not improve the distribution of the operator");
		}
	}
}