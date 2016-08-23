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
import backtype.storm.scheduler.WorkerSlot;
import storm.autoscale.scheduler.allocation.IAllocationStrategy;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;

/**
 * @author Roland
 *
 */
public class ScaleInAction implements IAction {

	private ComponentMonitor compMonitor;
	private TopologyExplorer explorer;
	private AssignmentMonitor assignMonitor;
	private IAllocationStrategy allocStrategy;
	private String nimbusHost;
	private Integer nimbusPort;
	private ArrayList<String> validateActions;
	private Logger logger = Logger.getLogger("ScaleInAction");
	
	/**
	 * 
	 */
	public ScaleInAction(ComponentMonitor compMonitor, TopologyExplorer explorer,
			AssignmentMonitor assignMonitor, IAllocationStrategy allocStrategy, String nimbusHost,
			Integer nimbusPort) {
		this.compMonitor = compMonitor;
		this.explorer = explorer;
		this.assignMonitor = assignMonitor;
		this.allocStrategy = allocStrategy;
		this.nimbusHost = nimbusHost;
		this.nimbusPort = nimbusPort;
		this.validateActions = new ArrayList<>();
		Thread thread = new Thread(this);
		thread.start();
	}

	@Override
	public void validate() {
		ArrayList<String> scaleInRequirements = this.compMonitor.getScaleInDecisions();
		for(String component : scaleInRequirements){
			boolean validate = true;
			Double eprValue = this.compMonitor.getEPRValue(component);
			if(eprValue == -1.0){
				validate = false;
				break;
			}
			ArrayList<String> antecedents = explorer.getAntecedents(component);
			for(String antecedent : antecedents){
				if(!this.explorer.getSpouts().contains(antecedent)){
					Double antecedentEprValue = this.compMonitor.getEPRValue(antecedent);
					if(antecedentEprValue >= 1 || this.compMonitor.needScaleOut(antecedent)){
						validate = false;
						break;
					}
				}
			}
			if(validate){
				this.validateActions.add(component);
			}
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		this.validate();
		//HashMap<String, WorkerSlot> bestWorkers = this.getBestLocation();
		TSocket tsocket = new TSocket(this.nimbusHost, this.nimbusPort);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);
		try {
			if(!tTransport.isOpen()){
				tTransport.open();
			}
			for(String component : this.validateActions){
				Double eprValue = this.compMonitor.getEPRValue(component);
	
				int maxParallelism = this.assignMonitor.getAllSortedTasks(component).size();

				int currentParallelism = this.assignMonitor.getParallelism(component);
				int estimatedParallelism  = (int) Math.round(eprValue * currentParallelism);
		
				int newParallelism = Math.min(maxParallelism, estimatedParallelism);
				
				if(newParallelism < currentParallelism && currentParallelism > 1){
					RebalanceOptions options = new RebalanceOptions();
					options.put_to_num_executors(component, newParallelism);
					options.set_num_workers(this.assignMonitor.getNbWorkers());
					options.set_wait_secs(0);

					logger.fine("Changing parallelism degree of component " + component + " from " + currentParallelism + " to " + newParallelism + "...");

					client.rebalance(explorer.getTopologyName(), options);
					logger.fine("Parallelism of component " + component + " decreased successfully!");
				}else{
					logger.fine("This scale-in will not decrease the distribution of the operator");
				}
				Thread.sleep(2000);
			}
		}catch (TException | InterruptedException e) {
			logger.severe("Unable to scale topology " + explorer.getTopologyName() + " because of " + e);
		}
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.actions.IAction#getBestLocation()
	 */
	@Override
	public HashMap<String, WorkerSlot> getBestLocation() {
		HashMap<String, WorkerSlot> result = new HashMap<>();
		for(String component : this.validateActions){
			WorkerSlot bestWorker = null;
			Double bestScore = Double.NEGATIVE_INFINITY;
			HashMap<WorkerSlot, Double> scores = this.allocStrategy.getScores(component);
			for(WorkerSlot ws : scores.keySet()){
				Double score = scores.get(ws);
				if(score > bestScore){
					bestWorker = ws;
					bestScore = score;
				}
			}
			result.put(component, bestWorker);
		}
		return result;
	}

}