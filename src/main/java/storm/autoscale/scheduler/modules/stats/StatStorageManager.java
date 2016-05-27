/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TFramedTransport;

import backtype.storm.generated.BoltStats;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.SpoutStats;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import storm.autoscale.scheduler.modules.listener.NimbusListener;

/**
 * @author Roland
 *
 */
public class StatStorageManager implements Runnable{

	private static StatStorageManager manager = null;
	private NimbusListener listener;
	private final Connection connection;
	private final Statement statement;
	private static Integer timestamp = 0;
	private final static String ALLTIME = ":all-time";
	private static Logger logger = Logger.getLogger("StatStorageManager");
	
	/**
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * 
	 */
	private StatStorageManager(String dbHost, String nimbusHost, Integer nimbusPort) throws SQLException, ClassNotFoundException {
		String jdbcDriver = "com.mysql.jdbc.Driver";
		String dbUrl = "jdbc:mysql://"+ dbHost +"/benchmarks";
		String user = "root";
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(dbUrl,user, null);
		this.statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		this.listener = new NimbusListener(nimbusHost, nimbusPort);
	}
	
	public static StatStorageManager getManager(String dbHost, String nimbusHost, Integer nimbusPort) throws ClassNotFoundException, SQLException{
		if(StatStorageManager.manager == null){
			StatStorageManager.manager = new StatStorageManager(dbHost, nimbusHost, nimbusPort);
		}
		return StatStorageManager.manager;
	}
	
	public void storeStatistics(){
		TFramedTransport tTransport = this.listener.gettTransport();
		Nimbus.Client client = this.listener.getClient();
		try {
			tTransport.open();
			List<TopologySummary> topologies = client.getClusterInfo().get_topologies();
			for(TopologySummary topSummary : topologies){
				TopologyInfo topology = client.getTopologyInfo(topSummary.get_id());
				List<ExecutorSummary> executors = topology.get_executors();
				for(ExecutorSummary executor : executors){
					String componentId = executor.get_component_id();
					String host = executor.get_host();
					Integer port = executor.get_port();
					
					ExecutorInfo info = executor.get_executor_info();
					Integer startTask = info.get_task_start();
					Integer endTask = info.get_task_end();
					ExecutorStats stats = executor.get_stats();
					
					/*Get outputs independently of the output stream and from the start of the topology*/
					Map<String, Long> emitted = stats.get_emitted().get(ALLTIME);
					Long outputs = 0L;
					for(String stream : emitted.keySet()){
						outputs += emitted.get(stream);
					}
					
					ExecutorSpecificStats specStats = stats.get_specific();
					
					/*Try to look if it is a spout*/
					SpoutStats spoutStats = specStats.get_spout();
					if(spoutStats != null){
						Map<String, Long> acked = spoutStats.get_acked().get(ALLTIME);
						Long throughput = 0L;
						for(String stream : acked.keySet()){
							throughput += acked.get(stream);
						}
						
						Map<String, Long> failed = spoutStats.get_failed().get(ALLTIME);
						Long losses = 0L;
						for(String stream : failed.keySet()){
							losses += failed.get(stream);
						}
						
						Map<String, Double> completeAvgTime = spoutStats.get_complete_ms_avg().get(ALLTIME);
						Double sum = 0.0;
						Double count = 0.0;
						for(String stream : completeAvgTime.keySet()){
							sum += completeAvgTime.get(stream);
							count++;
						}
						Double avgLatency = sum / count;
						storeSpoutExecutorStats(host, port, topology.get_id(), componentId, startTask, endTask, outputs, throughput, losses, avgLatency);
					}else{
						BoltStats boltStats = specStats.get_bolt();
						if(boltStats != null){
							Map<GlobalStreamId, Long> executed = boltStats.get_executed().get(ALLTIME);
							Long nbExecuted = 0L;
							for(GlobalStreamId gs : executed.keySet()){
								nbExecuted += executed.get(gs);
							}
							
							Map<GlobalStreamId, Double> executionAvgTime = boltStats.get_execute_ms_avg().get(ALLTIME);
							Double sum = 0.0;
							Double count = 0.0;
							for(GlobalStreamId gs : executionAvgTime.keySet()){
								sum += executionAvgTime.get(gs);
								count++;
							}
							Double avgLatency = sum / count;
							
							Double selectivity = outputs / (nbExecuted * 1.0);
							storeBoltExecutorStats(host, port, topology.get_id(), componentId, startTask, endTask, nbExecuted, outputs, avgLatency, selectivity);
						}else{
							logger.warning("Unable to identify the type of the operator");
						}
					}
				}
			}
		}catch(TException exception){
			exception.printStackTrace();
		}
	}			

	public void storeSpoutExecutorStats(String host, Integer port, String topology, String component, Integer startTask, Integer endTask, Long outputs, Long throughput, Long losses, Double avgLatency){
		//TODO store the values and add the timestamp
	}
	
	public void storeBoltExecutorStats(String host, Integer port, String topology, String component, Integer startTask, Integer endTask, Long executed, Long outputs, Double avgLatency, Double selectivity){
		//TODO store the values and add the timstamp
	}

	public ArrayList<String> getWorkers(String component){
		ArrayList<String> workers = new ArrayList<>();
		//TODO Look for the component in spout and bolt tables, if there is some results, for each row, add the host+port values into workers
		return workers;
	}
	
	public Long getExecuted(String component){
		Long result = 0L;
		//TODO Look in the bolt table all bolts related to the component and aggregate their executed values into result
		return result;
	}
	
	public Long getPreviousExecuted(String component){
		Long result = 0L;
		//TODO Look in the bolt table all bolts related to the component and aggregate their executed values into result
		return result;
	}
	
	public Long getOutputs(String component){
		Long result = 0L;
		//TODO Look in the spout and bolt tables all executors related to the component and if the result is not null, aggregate their outputs values into result
		return result;
	}
	
	public Long getPreviousOutputs(String component){
		Long result = 0L;
		//TODO Look in the spout and bolt tables all executors related to the component and if the result is not null, aggregate their outputs values into result
		return result;
	}
	
	public Double getAvgLatency(String component){
		Double result = 0.0;
		//TODO Look in the bolt table all bolts related to the component and compute the average value of their latencies, pick it as the result 
		return result;
	}
	
	public Double getSelectivity(String component){
		Double result = 0.0;
		//TODO Look in the bolt table all bolts related to the component and compute the average value of their selectivities, pick it as the result 
		return result;
	}
	
	public Double getPreviousSelectivity(String component){
		Double result = 0.0;
		//TODO Look in the bolt table all bolts related to the component and compute the average value of their selectivities, pick it as the result 
		return result;
	}
	
	public Long getTopologyThroughput(String topology){
		Long result = 0L;
		//TODO Look in the spout table all spouts related to the topology and aggregate their throughput values into result
		return result;
	}
	
	public Long getPreviousTopologyThroughput(String topology){
		Long result = 0L;
		//TODO Look in the spout table all spouts related to the topology and aggregate their throughput values into result
		return result;
	}
	
	public Long getTopologyLosses(String topology){
		Long result = 0L;
		//TODO Look in the spout table all spouts related to the topology and aggregate their losses values into result
		return result;
	}
	
	public Long getPreviousTopologyLosses(String topology){
		Long result = 0L;
		//TODO Look in the spout table all spouts related to the topology and aggregate their losses values into result
		return result;
	}
	
	public Double getTopologyAvgLatency(String topology){
		Double result = 0.0;
		//TODO Look in the spout table all spouts related to the topology and order them by decreasing average latency, pick up the first one as the result 
		return result;
	}
	
	@Override
	public void run() {
		storeStatistics();
		try {
			Thread.sleep(1000);
			timestamp++;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}	
}