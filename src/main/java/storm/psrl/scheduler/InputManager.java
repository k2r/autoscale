package storm.psrl.scheduler;

import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.NimbusClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;

import java.util.Map;

/**
 *
 * @author Roland
 */
public class InputManager {
    
    private Topologies topologies;
    @SuppressWarnings("rawtypes")
	private Map conf;
    private String host;
    private HashMap<String, ArrayList<ExecutorDetails>> compToExecutors;
    private double omega;
    private String logPath;
    
    
    @SuppressWarnings("rawtypes")
	public InputManager(Topologies topologies, Map conf, String host) throws IOException, AuthorizationException, org.apache.thrift7.TException{
        this.topologies = topologies;
        this.conf = conf;
        this.host = host;
        this.compToExecutors = new HashMap<>();
        this.omega = 5.0;
        this.logPath = "rates.log";
        Path path = Paths.get(this.logPath);
        List<String> logs = new ArrayList<>();
        if(Files.exists(path)){
            logs = Files.readAllLines(path);
            logs.add("-----------------------------------------------------------------------------------------------------");
        }else{
            Files.createFile(path);
        }
        int lastIndex = logs.size() - 1;
        NimbusClient client = new NimbusClient(this.conf, this.host);
        for(TopologyDetails td : this.topologies.getTopologies()){
            Map<ExecutorDetails, String> executorToComponent = td.getExecutorToComponent();
            for(ExecutorDetails ed : executorToComponent.keySet()){
                String component = executorToComponent.get(ed);
                if(!this.compToExecutors.containsKey(component)){
                    ArrayList<ExecutorDetails> executors = new ArrayList<>();
                    executors.add(ed);
                    this.compToExecutors.put(component, executors);
                }else{
                    ArrayList<ExecutorDetails> executors = this.compToExecutors.get(component);
                    executors.add(ed);
                    this.compToExecutors.replace(component, executors);
                }
            }
            TopologyInfo info = client.getClient().getTopologyInfo(td.getId());
            StormTopology st = td.getTopology();
            Map<String, SpoutSpec> spouts = st.get_spouts();
            Map<String, Bolt> bolts = st.get_bolts();
            for(String component : this.compToExecutors.keySet()){
                Double oldRate = null;
                for(int i = lastIndex; i > 0; i--){
                	String line = logs.get(i);
                    if(line.startsWith(component)){
                        oldRate = Double.parseDouble(line.split("@")[1]);
                        break;
                    }
                }
                Double globalRate = 0.0;
                List<ExecutorSummary> executors = info.get_executors();
                for (ExecutorSummary es : executors) {
                	String compId = es.get_component_id();
                	if(compId != null && es.get_stats() != null){
                		if(compId.equalsIgnoreCase(component) && Double.isFinite(es.get_stats().get_rate())){
                			globalRate += es.get_stats().get_rate();
                		}
                	}
                }
                if(oldRate != null){
                	double factor = 1;
                	if(globalRate != 0){
                		factor = oldRate / globalRate;
                	}
                    if(oldRate < (globalRate - omega)){
                        if(spouts.containsKey(component)){
                            SpoutSpec specs = spouts.get(component);
                            int newParDegree = Math.max(1, (int) Math.round(specs.get_common().get_parallelism_hint() * factor));
                            specs.get_common().set_parallelism_hint(newParDegree);
                            ArrayList<ExecutorDetails> toFork = compToExecutors.get(component);
                            for(ExecutorDetails ed : toFork){
                            	executorToComponent.remove(ed);
                            }
                            ExecutorDetails firstExec = toFork.get(0);
                            ExecutorDetails lastExec = toFork.get(toFork.size() - 1);
                            int taskRange = Math.abs(lastExec.getEndTask() - firstExec.getStartTask()) + 1;
                            int taskPerExec = Math.round(taskRange / newParDegree);
                            int execToAdd = taskRange / taskPerExec;
                            for(int i = 0; i < execToAdd; i++){
                            	int startTask = firstExec.getStartTask() + (taskPerExec * i);
                            	int endTask = startTask + taskPerExec - 1;
                            	ExecutorDetails fork = new ExecutorDetails(startTask, endTask);
                            	executorToComponent.put(fork, component);
                            }
                           
                        }
                        if(bolts.containsKey(component)){
                            Bolt specs = bolts.get(component);
                            int newParDegree = Math.max(1, (int) Math.round(specs.get_common().get_parallelism_hint() * factor));
                            specs.get_common().set_parallelism_hint(newParDegree);
                            ArrayList<ExecutorDetails> toFork = compToExecutors.get(component);
                            for(ExecutorDetails ed : toFork){
                            	executorToComponent.remove(ed);
                            }
                            ExecutorDetails firstExec = toFork.get(0);
                            ExecutorDetails lastExec = toFork.get(toFork.size() - 1);
                            int taskRange = Math.abs(lastExec.getEndTask() - firstExec.getStartTask()) + 1;
                            int taskPerExec = Math.round(taskRange / newParDegree);
                            int execToAdd = taskRange / taskPerExec;
                            for(int i = 0; i < execToAdd; i++){
                            	int startTask = firstExec.getStartTask() + (taskPerExec * i);
                            	int endTask = startTask + taskPerExec - 1;
                            	ExecutorDetails fork = new ExecutorDetails(startTask, endTask);
                            	executorToComponent.put(fork, component);
                            }
                        }
                    }
                    if(oldRate > (globalRate + omega)){
                        if(spouts.containsKey(component)){
                            SpoutSpec specs = spouts.get(component);
                            int newParDegree = Math.max(1, (int) Math.round(specs.get_common().get_parallelism_hint() * factor));
                            specs.get_common().set_parallelism_hint(newParDegree);
                            
                            ArrayList<ExecutorDetails> toMerge = compToExecutors.get(component);
                            for(ExecutorDetails ed : toMerge){
                            	executorToComponent.remove(ed);
                            }
                            ExecutorDetails firstExec = toMerge.get(0);
                            ExecutorDetails lastExec = toMerge.get(toMerge.size() - 1);
                            int taskRange = Math.abs(lastExec.getEndTask() - firstExec.getStartTask()) + 1;
                            int taskPerExec = Math.round(taskRange / newParDegree);
                            int execToAdd = taskRange / taskPerExec;
                            for(int i = 0; i < execToAdd; i++){
                            	int startTask = firstExec.getStartTask() + (taskPerExec * i);
                            	int endTask = startTask + taskPerExec - 1;
                            	ExecutorDetails fork = new ExecutorDetails(startTask, endTask);
                            	executorToComponent.put(fork, component);
                            }
                        }
                        if(bolts.containsKey(component)){
                            Bolt specs = bolts.get(component);
                            int newParDegree = Math.max(1, (int) Math.round(specs.get_common().get_parallelism_hint() * factor));
                            specs.get_common().set_parallelism_hint(newParDegree);
                            
                            ArrayList<ExecutorDetails> toMerge = compToExecutors.get(component);
                            for(ExecutorDetails ed : toMerge){
                            	executorToComponent.remove(ed);
                            }
                            ExecutorDetails firstExec = toMerge.get(0);
                            ExecutorDetails lastExec = toMerge.get(toMerge.size() - 1);
                            int taskRange = Math.abs(lastExec.getEndTask() - firstExec.getStartTask()) + 1;
                            int taskPerExec = Math.round(taskRange / newParDegree);
                            int execToAdd = taskRange / taskPerExec;
                            for(int i = 0; i < execToAdd; i++){
                            	int startTask = firstExec.getStartTask() + (taskPerExec * i);
                            	int endTask = startTask + taskPerExec - 1;
                            	ExecutorDetails fork = new ExecutorDetails(startTask, endTask);
                            	executorToComponent.put(fork, component);
                            }
                        }
                    }
                }
                logs.add(component + "@" + globalRate);
            }
            Files.write(path, logs, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        }
        
    }
}