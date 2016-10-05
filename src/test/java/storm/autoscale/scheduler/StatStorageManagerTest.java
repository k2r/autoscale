/**
 * 
 */
package storm.autoscale.scheduler;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.TestCase;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;

/**
 * @author Roland
 *
 */
public class StatStorageManagerTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#storeSpoutExecutorStats(java.lang.Integer, java.lang.String, java.lang.Integer, java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer, java.lang.Long, java.lang.Long, java.lang.Long, java.lang.Double)}.
	 */
	public void testStoreSpoutExecutorStats() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp = 0;
			String host = "testHost";
			Integer port = 0;
			String topology = "testTopology";
			String component = "testComponent";
			Integer startTask = 0;
			Integer endTask = 10;
			Long totalOutputs = 100L;
			Long totalThroughput = 50L;
			Long totalLosses = 5L;
			Long updateOutputs = 10L;
			Long updateThroughput = 8L;
			Long updateLosses = 0L;
			Double avgLatency = 500.0; 
			manager.storeSpoutExecutorStats(timestamp, host, port, topology, component, startTask, endTask, totalOutputs, updateOutputs, totalThroughput, updateThroughput, totalLosses, updateLosses, avgLatency);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			String testSpoutStorageQuery = "SELECT * FROM all_time_spouts_stats";
			ResultSet result = statement.executeQuery(testSpoutStorageQuery);
			
			Integer actualTimestamp = null;
			String actualHost = null;
			Integer actualPort = null;
			String actualTopology = null;
			String actualComponent = null;
			Integer actualStartTask = null;
			Integer actualEndTask = null;
			Long actualTotalOutputs = null;
			Long actualTotalThroughput = null;
			Long actualTotalLosses = null;
			Long actualUpdateOutputs = null;
			Long actualUpdateThroughput = null;
			Long actualUpdateLosses = null;
			Double actualAvgLatency = null;
			if(result.next()){
				actualTimestamp = result.getInt("timestamp");
				actualHost = result.getString("host");
				actualPort = result.getInt("port");
				actualTopology = result.getString("topology");
				actualComponent = result.getString("component");
				actualStartTask = result.getInt("start_task");
				actualEndTask = result.getInt("end_task");
				actualTotalOutputs = result.getLong("total_outputs");
				actualTotalThroughput = result.getLong("total_throughput");
				actualTotalLosses = result.getLong("total_losses");
				actualUpdateOutputs = result.getLong("update_outputs");
				actualUpdateThroughput = result.getLong("update_throughput");
				actualUpdateLosses = result.getLong("update_losses");
				actualAvgLatency = result.getDouble("complete_ms_avg");
			}
			assertEquals(timestamp, actualTimestamp, 0);
			assertEquals(host, actualHost);
			assertEquals(port, actualPort, 0);
			assertEquals(topology, actualTopology);
			assertEquals(component, actualComponent);
			assertEquals(startTask, actualStartTask, 0);
			assertEquals(endTask, actualEndTask, 0);
			assertEquals(totalOutputs, actualTotalOutputs, 0);
			assertEquals(totalThroughput, actualTotalThroughput, 0);
			assertEquals(totalLosses, actualTotalLosses, 0);
			assertEquals(updateOutputs, actualUpdateOutputs, 0);
			assertEquals(updateThroughput, actualUpdateThroughput, 0);
			assertEquals(updateLosses, actualUpdateLosses, 0);
			assertEquals(avgLatency, actualAvgLatency, 0);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#storeBoltExecutorStats(java.lang.Integer, java.lang.String, java.lang.Integer, java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer, java.lang.Long, java.lang.Long, java.lang.Double, java.lang.Double)}.
	 */
	public void testStoreBoltExecutorStats() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp = 0;
			String host = "testHost";
			Integer port = 0;
			String topology = "testTopology";
			String component = "testComponent";
			Integer startTask = 0;
			Integer endTask = 10;
			Long totalExecuted = 100L;
			Long totalOutputs = 80L;
			Long updateExecuted = 10L;
			Long updateOutputs = 8L;
			Double avgLatency = 500.0;
			Double selectivity = 0.8;
			manager.storeBoltExecutorStats(timestamp, host, port, topology, component, startTask, endTask, totalExecuted, updateExecuted, totalOutputs, updateOutputs, avgLatency, selectivity);

			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);

			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			String testBolttStorageQuery = "SELECT * FROM all_time_bolts_stats";
			ResultSet result = statement.executeQuery(testBolttStorageQuery);

			Integer actualTimestamp = null;
			String actualHost = null;
			Integer actualPort = null;
			String actualTopology = null;
			String actualComponent = null;
			Integer actualStartTask = null;
			Integer actualEndTask = null;
			Long actualTotalExecuted = null;
			Long actualTotalOutputs = null;
			Long actualUpdateExecuted = null;
			Long actualUpdateOutputs = null;
			Double actualAvgLatency = null;
			Double actualSelectivity = null;
			if(result.next()){
				actualTimestamp = result.getInt("timestamp");
				actualHost = result.getString("host");
				actualPort = result.getInt("port");
				actualTopology = result.getString("topology");
				actualComponent = result.getString("component");
				actualStartTask = result.getInt("start_task");
				actualEndTask = result.getInt("end_task");
				actualTotalExecuted = result.getLong("total_executed");
				actualTotalOutputs = result.getLong("total_outputs");
				actualUpdateExecuted = result.getLong("update_executed");
				actualUpdateOutputs = result.getLong("update_outputs");
				actualAvgLatency = result.getDouble("execute_ms_avg");
				actualSelectivity = result.getDouble("selectivity");
			}
			assertEquals(timestamp, actualTimestamp, 0);
			assertEquals(host, actualHost);
			assertEquals(port, actualPort, 0);
			assertEquals(topology, actualTopology);
			assertEquals(component, actualComponent);
			assertEquals(startTask, actualStartTask, 0);
			assertEquals(endTask, actualEndTask, 0);
			assertEquals(totalExecuted, actualTotalExecuted, 0);
			assertEquals(totalOutputs, actualTotalOutputs, 0);
			assertEquals(updateExecuted, actualUpdateExecuted, 0);
			assertEquals(updateOutputs, actualUpdateOutputs, 0);
			assertEquals(avgLatency, actualAvgLatency, 0);
			assertEquals(selectivity, actualSelectivity, 0);

			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#storeActivityInfo(java.lang.Integer, java.lang.String, java.lang.String, java.lang.String, java.lang.Double, java.lang.Integer, java.lang.Double)}.
	 */
	public void testStoreActivityInfo() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp = 1;
			String topology = "testTopology";
			String component = "testComponent";
			Double activityValue = 0.85;
			Integer remaining = 50;
			Double processingRate = 30.0;
			
			manager.storeActivityInfo(timestamp, topology, component, activityValue, remaining, processingRate);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);

			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			String testCRStorageQuery = "SELECT * FROM operators_activity";
			ResultSet result = statement.executeQuery(testCRStorageQuery);
			
			Integer actualTimestamp = null;
			String actualTopology = null;
			String actualComponent = null;
			Double actualCRValue = null;
			Integer actualRemaining = null;
			Double actualProcessingRate = null;
			if(result.next()){
				actualTimestamp = result.getInt("timestamp");
				actualTopology = result.getString("topology");
				actualComponent = result.getString("component");
				actualCRValue = result.getDouble("activity_level");
				actualRemaining = result.getInt("remaining_tuples");
				actualProcessingRate = result.getDouble("capacity_per_second");
			}
			
			assertEquals(timestamp, actualTimestamp);
			assertEquals(topology, actualTopology);
			assertEquals(component, actualComponent);
			assertEquals(activityValue, actualCRValue);
			assertEquals(remaining, actualRemaining);
			assertEquals(processingRate, actualProcessingRate);
			
			String testCleanQuery = "DELETE FROM operators_activity";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getWorkers(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetWorkers() {
		try {
			
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2);
			
			HashMap<Integer, ArrayList<String>> actualWorkers = manager.getBoltWorkers(component, 11, 10);
			
			ArrayList<String> expectedWorkersTimestamp1 = new ArrayList<>();
			expectedWorkersTimestamp1.add(host1 + "@" + port1);
			expectedWorkersTimestamp1.add(host2 + "@" + port2);
			
			ArrayList<String> expectedWorkersTimestamp2 = new ArrayList<>();
			expectedWorkersTimestamp2.add(host2 + "@" + port2);
			
			HashMap<Integer, ArrayList<String>> expectedWorkers = new HashMap<>();
			expectedWorkers.put(timestamp1, expectedWorkersTimestamp1);
			expectedWorkers.put(timestamp2, expectedWorkersTimestamp2);
			assertEquals(expectedWorkers, actualWorkers);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getExecuted(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetExecuted() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			Long updateExecuted3 = 12L;
			Long updateExecuted4 = 7L;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted3, totalOutputs1, updateOutputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted4, totalOutputs2, updateOutputs2, avgLatency2, selectivity2);
			
			HashMap<Integer, Long> actualExecuted = manager.getExecuted(component, 11, 10);
			
			HashMap<Integer, Long> expectedExecuted = new HashMap<>();
			Long expectedExecutedTimestamp1 = updateExecuted1 + updateExecuted2;
			Long expectedExecutedTimestamp2 = updateExecuted3 + updateExecuted4;
			expectedExecuted.put(timestamp1, expectedExecutedTimestamp1);
			expectedExecuted.put(timestamp2, expectedExecutedTimestamp2);
			
			assertEquals(expectedExecuted, actualExecuted);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getOutputs(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetOutputs() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			Long updateOutputs3 = 100L;
			Long updateOutputs4 = 75L;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs3, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs4, avgLatency2, selectivity2);
			
			HashMap<Integer, Long> actualOutputs = manager.getBoltOutputs(component, 11, 10);
			
			HashMap<Integer, Long> expectedOutputs = new HashMap<>();
			Long expectedOutputsTimestamp1 = updateOutputs1 + updateOutputs2;
			Long expectedOutputsTimestamp2 = updateOutputs3 + updateOutputs4;
			expectedOutputs.put(timestamp1, expectedOutputsTimestamp1);
			expectedOutputs.put(timestamp2, expectedOutputsTimestamp2);
			
			assertEquals(expectedOutputs, actualOutputs);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getAvgLatency(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetAvgLatency() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			Double avgLatency3 = 65.0;
			Double avgLatency4 = 72.0;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency3, selectivity1);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency4, selectivity2);
			
			HashMap<Integer, Double> actualAvgLatency = manager.getAvgLatency(component, 11, 10);
			
			HashMap<Integer, Double> expectedAvgLatency = new HashMap<>();
			Double expectedAvgLatencyTimestamp1 = (avgLatency1 + avgLatency2) / 2;
			Double expectedAvgLatencyTimestamp2 = (avgLatency3 + avgLatency4) / 2;
			expectedAvgLatency.put(timestamp1, expectedAvgLatencyTimestamp1);
			expectedAvgLatency.put(timestamp2, expectedAvgLatencyTimestamp2);
			
			assertEquals(expectedAvgLatency, actualAvgLatency);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getSelectivity(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetSelectivity() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalExecuted1 = 100L;
			Long totalOutputs1 = 80L;
			Long updateExecuted1 = 10L;
			Long updateOutputs1 = 8L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalExecuted2 = 100L;
			Long totalOutputs2 = 70L;
			Long updateExecuted2 = 10L;
			Long updateOutputs2 = 7L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			Double selectivity3 = 0.76;
			Double selectivity4 = 0.83;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalExecuted1, updateExecuted1, totalOutputs1, updateOutputs1, avgLatency1, selectivity3);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalExecuted2, updateExecuted2, totalOutputs2, updateOutputs2, avgLatency2, selectivity4);
			
			HashMap<Integer, Double> actualSelectivity = manager.getSelectivity(component, 11, 10);
			
			HashMap<Integer, Double> expectedSelectivity = new HashMap<>();
			Double expectedSelectivityTimestamp1 = new BigDecimal((selectivity1 + selectivity2) / 2).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
			Double expectedSelectivityTimestamp2 = new BigDecimal((selectivity3 + selectivity4) / 2).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
			expectedSelectivity.put(timestamp1, expectedSelectivityTimestamp1);
			expectedSelectivity.put(timestamp2, expectedSelectivityTimestamp2);
			
			assertEquals(expectedSelectivity, actualSelectivity);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getTopologyThroughput(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyThroughput() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp1 = 1;
			Integer timestamp2 = 9;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalOutputs1 = 100L;
			Long totalThroughput1 = 50L;
			Long totalLosses1 = 5L;
			Long updateOutputs1 = 10L;
			Long updateThroughput1 = 8L;
			Long updateLosses1 = 0L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalOutputs2 = 100L;
			Long totalThroughput2 = 50L;
			Long totalLosses2 = 5L;
			Long updateOutputs2 = 10L;
			Long updateThroughput2 = 8L;
			Long updateLosses2 = 2L;
			Double avgLatency2 = 700.0;
			
			Long updateThroughput3 = 12L;
			Long updateThroughput4 = 14L;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput3, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput4, totalLosses2, updateLosses2, avgLatency2);
			
			HashMap<Integer, Long> actualThroughput = manager.getTopologyThroughput(topology, 11, 10);
			
			HashMap<Integer, Long> expectedThroughput = new HashMap<>();
			Long expectedThroughputTimestamp1 = updateThroughput1 + updateThroughput2;
			Long expectedThroughputTimestamp2 = updateThroughput3 + updateThroughput4;
			expectedThroughput.put(timestamp1, expectedThroughputTimestamp1);
			expectedThroughput.put(timestamp2, expectedThroughputTimestamp2);
			
			assertEquals(expectedThroughput, actualThroughput);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getTopologyLosses(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyLosses() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalOutputs1 = 100L;
			Long totalThroughput1 = 50L;
			Long totalLosses1 = 5L;
			Long updateOutputs1 = 10L;
			Long updateThroughput1 = 8L;
			Long updateLosses1 = 0L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalOutputs2 = 100L;
			Long totalThroughput2 = 50L;
			Long totalLosses2 = 5L;
			Long updateOutputs2 = 10L;
			Long updateThroughput2 = 8L;
			Long updateLosses2 = 2L;
			Double avgLatency2 = 700.0;
			
			Long updateLosses3 = 4L;
			Long updateLosses4 = 5L;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses3, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses4, avgLatency2);
			
			HashMap<Integer, Long> actualLosses = manager.getTopologyLosses(topology, 11, 10);
			
			HashMap<Integer, Long> expectedLosses = new HashMap<>();
			Long expectedLossesTimestamp1 = updateLosses1 + updateLosses2;
			Long expectedLossesTimestamp2 = updateLosses3 + updateLosses4;
			expectedLosses.put(timestamp1, expectedLossesTimestamp1);
			expectedLosses.put(timestamp2, expectedLossesTimestamp2);
			
			assertEquals(expectedLosses, actualLosses);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.StatStorageManager#getTopologyAvgLatency(java.lang.String, java.lang.Integer)}.
	 */
	public void testGetTopologyAvgLatency() {
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long totalOutputs1 = 100L;
			Long totalThroughput1 = 50L;
			Long totalLosses1 = 5L;
			Long updateOutputs1 = 10L;
			Long updateThroughput1 = 8L;
			Long updateLosses1 = 0L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long totalOutputs2 = 100L;
			Long totalThroughput2 = 50L;
			Long totalLosses2 = 5L;
			Long updateOutputs2 = 10L;
			Long updateThroughput2 = 8L;
			Long updateLosses2 = 2L;
			Double avgLatency2 = 700.0;
			
			Double avgLatency3 = 460.0;
			Double avgLatency4 = 332.0;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency3);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency4);
			
			HashMap<Integer, Double> actualAvgLatency = manager.getTopologyAvgLatency(topology, 11, 10);
			
			HashMap<Integer, Double> expectedAvgLatency = new HashMap<>();
			Double expectedAvgLatencyTimestamp1 = Math.max(avgLatency1, avgLatency2);
			Double expectedAvgLatencyTimestamp2 = Math.max(avgLatency3, avgLatency4);
			expectedAvgLatency.put(timestamp1, expectedAvgLatencyTimestamp1);
			expectedAvgLatency.put(timestamp2, expectedAvgLatencyTimestamp2);
			
			assertEquals(expectedAvgLatency, actualAvgLatency);
			
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);
			
			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	public void testGetFormerValue(){
		try {
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);

			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			Integer timestamp3 = 21;
			Integer timestamp4 = 22; 
			String topology = "testTopology";
			String component = "testComponent";

			String host1 = "testHost1";
			Integer port = 0;
			Integer startTask = 0;
			Integer endTask = 10;
			Long totalOutputs1 = 100L;
			Long totalThroughput1 = 50L;
			Long totalLosses1 = 5L;
			Long updateOutputs1 = 10L;
			Long updateThroughput1 = 8L;
			Long updateLosses1 = 0L;
			Double avgLatency1 = 500.0;


			String host2 = "testHost2";
			Long totalOutputs2 = 100L;
			Long totalThroughput2 = 50L;
			Long totalLosses2 = 5L;
			Long updateOutputs2 = 10L;
			Long updateThroughput2 = 8L;
			Long updateLosses2 = 2L;
			Double avgLatency2 = 700.0;

			Long totalOutputs3 = 150L;
			Long totalThroughput3 = 90L;
			Long totalLosses3 = 15L;
			Long updateOutputs3 = 15L;
			Long updateThroughput3 = 9L;
			Long updateLosses3 = 1L;
			Double avgLatency3 = 700.0;

			manager.storeSpoutExecutorStats(timestamp1, host1, port, topology, component, startTask, endTask, totalOutputs1, updateOutputs1, totalThroughput1, updateThroughput1, totalLosses1, updateLosses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp2, host1, port, topology, component, startTask, endTask, totalOutputs2, updateOutputs2, totalThroughput2, updateThroughput2, totalLosses2, updateLosses2, avgLatency2);
			manager.storeSpoutExecutorStats(timestamp3, host2, port, topology, component, startTask, endTask, totalOutputs3, updateOutputs3, totalThroughput3, updateThroughput3, totalLosses3, updateLosses3, avgLatency3);

			Long actual1 = manager.getFormerValue(component, startTask, endTask, timestamp1, "spout", "total_outputs");
			Long actual2 = manager.getFormerValue(component, startTask, endTask, timestamp2, "spout", "total_outputs");
			Long actual3 = manager.getFormerValue(component, startTask, endTask, timestamp3, "spout", "update_outputs");
			Long actual4 = manager.getFormerValue(component, startTask, endTask, timestamp4, "spout", "update_outputs");
			Long actual5 = manager.getFormerValue(component, startTask, endTask, timestamp4, "spout", "update_losses");

			assertEquals(0L, actual1, 0);
			assertEquals(100L, actual2, 0);
			assertEquals(10L, actual3, 0);
			assertEquals(15L, actual4, 0);
			assertEquals(1L, actual5, 0);

			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			String user = "root";
			Class.forName(jdbcDriver);

			Connection connection = DriverManager.getConnection(dbUrl,user, null);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

			String testCleanQuery = "DELETE FROM all_time_spouts_stats";
			statement.executeUpdate(testCleanQuery);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void testGetFormerRemainingTuples(){
		try {
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String dbUrl = "jdbc:mysql://localhost/benchmarks";
			Class.forName(jdbcDriver);
			Connection connection = DriverManager.getConnection(dbUrl, "root", null);
			ArrayList<String> queries = new ArrayList<>();
			String queryA1 = "INSERT INTO operators_activity VALUES('1', 'topologyTest', 'A', '1', '0', '10')";
			String queryB1 = "INSERT INTO operators_activity VALUES('1', 'topologyTest', 'B', '1', '5', '5')";
			String queryA2 = "INSERT INTO operators_activity VALUES('2', 'topologyTest', 'A', '1', '4', '10')";
			String queryB2 = "INSERT INTO operators_activity VALUES('2', 'topologyTest', 'B', '1', '3', '5')";
			String queryA3 = "INSERT INTO operators_activity VALUES('3', 'topologyTest', 'A', '1', '8', '10')";
			String queryB3 = "INSERT INTO operators_activity VALUES('3', 'topologyTest', 'B', '1', '6', '10')";

			queries.add(queryA1);
			queries.add(queryB1);
			queries.add(queryA2);
			queries.add(queryB2);
			queries.add(queryA3);
			queries.add(queryB3);
			
			for(String query : queries){
				try {
					Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
					statement.executeUpdate(query);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			StatStorageManager manager = StatStorageManager.getManager("localhost", null);
			
			assertEquals(0, manager.getFormerRemainingTuples(1, "A"), 0);
			assertEquals(0, manager.getFormerRemainingTuples(2, "A"), 0);
			assertEquals(4, manager.getFormerRemainingTuples(3, "A"), 0);
			assertEquals(8, manager.getFormerRemainingTuples(4, "A"), 0);
			assertEquals(0,manager.getFormerRemainingTuples(1, "B"), 0);
			assertEquals(5, manager.getFormerRemainingTuples(2, "B"), 0);
			assertEquals(3, manager.getFormerRemainingTuples(3, "B"), 0);
			assertEquals(6, manager.getFormerRemainingTuples(4, "B"), 0);
			
			String cleanQuery1 = "DELETE FROM all_time_spouts_stats";
			String cleanQuery2 = "DELETE FROM all_time_bolts_stats";
			String cleanQuery3 = "DELETE FROM operators_activity";
			String cleanQuery4 = "DELETE FROM topologies_status";
			
			ArrayList<String> cleanQueries = new ArrayList<>();
			cleanQueries.add(cleanQuery1);
			cleanQueries.add(cleanQuery2);
			cleanQueries.add(cleanQuery3);
			cleanQueries.add(cleanQuery4);
			
			for(String query : cleanQueries){
				try {
					Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
					statement.executeUpdate(query);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();		
		}	
	}
}
