package storm.psrl.scheduler;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.DefaultScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import storm.psrl.scheduler.util.ExecutorUIdentifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift7.*;

/**
 *
 * @author Roland
 */
public class PSRLScheduler implements IScheduler {

	@SuppressWarnings("rawtypes")
	private Map conf;
	private String host;
	private static Logger logger = Logger.getLogger("PSRLScheduler");
	InputManager iManager;
	ResourceMonitor rMonitor;
	private boolean isFirst;
	
	public PSRLScheduler(){
		logger.info("Initializing the PSRLScheduler on Storm...");
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) {
		this.conf = conf;
		this.host = (String)this.conf.get(Config.NIMBUS_HOST);
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		Path path = Paths.get("changeScheduler.log");
		if(Files.exists(path)){
			this.isFirst = false;
		}else{
			this.isFirst = true;
		}
		if(isFirst){
			DefaultScheduler scheduler = new DefaultScheduler();
			scheduler.schedule(topologies, cluster);
			if(!topologies.getTopologies().isEmpty()){
				Path changeScheduler = Paths.get("changeScheduler.log");
				try {
					Files.createFile(changeScheduler);
				} catch (IOException e) {
					PSRLScheduler.logger.severe("Unable to catch the scheduling switch file");
				}
			}
		}else{
			try {
				rMonitor = new ResourceMonitor(cluster);
				rMonitor.persistAllocations();
				iManager = new InputManager(topologies, conf, host);
				ArrayList<SupervisorDetails> underUsed = rMonitor.getUnderUsed();
				logger.info("The cluster has " + underUsed.size() + " underused host(s) :");
				for(SupervisorDetails uu : underUsed){
					logger.info("->" + uu);
				}
				ArrayList<SupervisorDetails> overUsed = rMonitor.getOverUsed();
				logger.info("The cluster has " + overUsed.size() + " overused host(s) :");
				for(SupervisorDetails ou : overUsed){
					logger.info("->" + ou);
				}
				manageSupervisor(cluster, underUsed, overUsed);
				List<WorkerSlot> candidates = cluster.getAvailableSlots();
				int nbBuckets = candidates.size();
				if(nbBuckets >  0){ // need to work only if there is at least an available worker
					ArrayList<ArrayList<ExecutorUIdentifier>> executorBuckets = new ArrayList<ArrayList<ExecutorUIdentifier>>();
					for(int i = 0; i < nbBuckets; i++){
						executorBuckets.add(new ArrayList<>());
					}
					makeBuckets(executorBuckets, topologies, cluster, nbBuckets);
					assignBuckets(executorBuckets, candidates, underUsed, overUsed, cluster, nbBuckets);
				}
			} catch (IOException | TException ex) {
				Logger.getLogger(PSRLScheduler.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	/**
	 * 
	 * @param cluster The instance which manage the pool of supervisors
	 * @param underUsed The list of supervisors using less resources than advised by the user
	 * @param overUsed The list of supervisors using at least one resource more than advised by the user
	 */
	public void manageSupervisor(Cluster cluster, ArrayList<SupervisorDetails> underUsed, ArrayList<SupervisorDetails> overUsed){
		Collection<WorkerSlot> workers = cluster.getUsedSlots();
		for(WorkerSlot ws : workers){
			String host = ws.getNodeId();
			logger.info("Checking host " + host);
			SupervisorDetails supervisor = cluster.getSupervisorById(host);
			if(underUsed.contains(supervisor)){
				cluster.freeSlot(ws);
				logger.info("Freeing an under-used slot on host " + host + "...");
			}
			if(overUsed.contains(supervisor)){
				cluster.freeSlot(ws);
				overUsed.remove(supervisor);//freeing a slot to decrease the node charge
				logger.info("Decreasing the charge of the over-used host " + host + "...");
			}
		}
	}
	
	/**
	 * This method affects executors to buckets in order to separate executors which are forks of the same component. It also takes into account the membership to a same topology to use as less worker as possible for a single topology.
	 * @param componentBuckets the list of executor buckets where each Executor is represented by its component id
	 * @param topologyBuckets the list of executor buckets where each Executor is represented by its topology id
	 * @param executorBuckets the list of executor buckets where each Executor is represented by its instance of ExecutorDetails
	 * @param topologies the list of topologies submitted to the cluster 
	 * @param cluster the instance which manage the pool of supervisors
	 * @param nbBuckets the number of buckets
	 */
	public void makeBuckets(ArrayList<ArrayList<ExecutorUIdentifier>> executorBuckets, Topologies topologies, Cluster cluster, Integer nbBuckets){
		for(TopologyDetails td : topologies.getTopologies()){
			HashMap<ExecutorDetails, String> execToComp = (HashMap<ExecutorDetails, String>) td.getExecutorToComponent();
			Collection<ExecutorDetails> execToAffect = cluster.getUnassignedExecutors(td);
			String topId = td.getId();
			for(ExecutorDetails ed : execToAffect){
				String compId = execToComp.get(ed);
				ExecutorUIdentifier eui = new ExecutorUIdentifier(ed, compId, topId);
				int i = 0;
				while(i < nbBuckets){
					ArrayList<ExecutorUIdentifier> bucket = executorBuckets.get(i);
					if(i >= (nbBuckets - 1)){
						bucket.add(eui);
						executorBuckets.set(i, bucket);
						break;
					}
					if(!eui.hasFork(bucket)){
						bucket.add(eui);
						executorBuckets.set(i, bucket);
						break;
					}
					i++;
				}
			}
		}
	}
	
	/**
	 * This method assigns executor buckets to available workers. It takes into account if workers are on over-used or under-used supervisors in order to avoid them as long as possible.
	 * @param componentBuckets the list of executor buckets where each Executor is represented by its component id
	 * @param topologyBuckets the list of executor buckets where each Executor is represented by its topology id
	 * @param executorBuckets the list of executor buckets where each Executor is represented by its instance of ExecutorDetails
	 * @param candidates the list of available slots
	 * @param underUsed The list of supervisors using less resources than advised by the user
	 * @param overUsed The list of supervisors using at least one resource more than advised by the user
	 * @param cluster the instance which manage the pool of supervisors
	 * @param nbBuckets the number of buckets
	 */
	public void assignBuckets(ArrayList<ArrayList<ExecutorUIdentifier>> executorBuckets, List<WorkerSlot> candidates, ArrayList<SupervisorDetails> underUsed, ArrayList<SupervisorDetails> overUsed, Cluster cluster, Integer nbBuckets){
		for(int i = 0; i < nbBuckets; i++){
			ArrayList<ExecutorUIdentifier> execBucket = executorBuckets.get(i);
			if(!execBucket.isEmpty()){
				Collection<ExecutorDetails> execDetails = (Collection<ExecutorDetails>) new ArrayList<ExecutorDetails>();
				for(ExecutorUIdentifier eui : execBucket){
					execDetails.add(eui.getEd());
				}
				String topId = execBucket.get(0).getTopId();
				for(WorkerSlot cand : candidates){
					SupervisorDetails supervisor = cluster.getSupervisorById(cand.getNodeId());
					if(!overUsed.contains(supervisor)){
						if(!underUsed.contains(supervisor)){
							cluster.assign(cand, topId, execDetails);
							candidates.remove(cand);
							logger.info("Rescheduled " + execBucket.size() + " executors on worker " + cand.getNodeId() + "@" + cand.getPort());
							break;
						}else{//if there is no available worker which is neither over used nor under used 
							cluster.assign(cand, topId, execDetails);
							candidates.remove(cand);
							logger.info("Rescheduled " + execBucket.size() + " executors on worker " + cand.getNodeId() + "@" + cand.getPort());
							break;
						}
					}
				}
			}
		}
	}
}
