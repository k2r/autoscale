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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String host = "testHost";
			Integer port = 0;
			String topology = "testTopology";
			String component = "testComponent";
			Integer startTask = 0;
			Integer endTask = 10;
			Long outputs = 100L;
			Long throughput = 50L;
			Long losses = 5L;
			Double avgLatency = 500.0; 
			manager.storeSpoutExecutorStats(timestamp, host, port, topology, component, startTask, endTask, outputs, throughput, losses, avgLatency);
			
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
			Long actualOutputs = null;
			Long actualThroughput = null;
			Long actualLosses = null;
			Double actualAvgLatency = null;
			if(result.next()){
				actualTimestamp = result.getInt("timestamp");
				actualHost = result.getString("host");
				actualPort = result.getInt("port");
				actualTopology = result.getString("topology");
				actualComponent = result.getString("component");
				actualStartTask = result.getInt("start_task");
				actualEndTask = result.getInt("end_task");
				actualOutputs = result.getLong("outputs");
				actualThroughput = result.getLong("throughput");
				actualLosses = result.getLong("losses");
				actualAvgLatency = result.getDouble("complete_ms_avg");
			}
			assertEquals(timestamp, actualTimestamp, 0);
			assertEquals(host, actualHost);
			assertEquals(port, actualPort, 0);
			assertEquals(topology, actualTopology);
			assertEquals(component, actualComponent);
			assertEquals(startTask, actualStartTask, 0);
			assertEquals(endTask, actualEndTask, 0);
			assertEquals(outputs, actualOutputs, 0);
			assertEquals(throughput, actualThroughput, 0);
			assertEquals(losses, actualLosses, 0);
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp = 0;
			String host = "testHost";
			Integer port = 0;
			String topology = "testTopology";
			String component = "testComponent";
			Integer startTask = 0;
			Integer endTask = 10;
			Long executed = 100L;
			Long outputs = 80L;
			Double avgLatency = 500.0;
			Double selectivity = 0.8;
			manager.storeBoltExecutorStats(timestamp, host, port, topology, component, startTask, endTask, executed, outputs, avgLatency, selectivity);

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
			Long actualExecuted = null;
			Long actualOutputs = null;
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
				actualExecuted = result.getLong("executed");
				actualOutputs = result.getLong("outputs");
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
			assertEquals(executed, actualExecuted, 0);
			assertEquals(outputs, actualOutputs, 0);
			assertEquals(avgLatency, actualAvgLatency, 0);
			assertEquals(selectivity, actualSelectivity, 0);

			String testCleanQuery = "DELETE FROM all_time_bolts_stats";
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
			
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 95L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			Long executed3 = 120L;
			Long executed4 = 90L;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, executed3, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, executed4, outputs2, avgLatency2, selectivity2);
			
			HashMap<Integer, Long> actualExecuted = manager.getExecuted(component, 11, 10);
			
			HashMap<Integer, Long> expectedExecuted = new HashMap<>();
			Long expectedExecutedTimestamp1 = executed1 + executed2;
			Long expectedExecutedTimestamp2 = executed3 + executed4;
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			Long outputs3 = 100L;
			Long outputs4 = 75L;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, executed1, outputs3, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, executed2, outputs4, avgLatency2, selectivity2);
			
			HashMap<Integer, Long> actualOutputs = manager.getBoltOutputs(component, 11, 10);
			
			HashMap<Integer, Long> expectedOutputs = new HashMap<>();
			Long expectedOutputsTimestamp1 = outputs1 + outputs2;
			Long expectedOutputsTimestamp2 = outputs3 + outputs4;
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			Double avgLatency3 = 65.0;
			Double avgLatency4 = 72.0;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency3, selectivity1);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency4, selectivity2);
			
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long executed1 = 100L;
			Long outputs1 = 80L;
			Double avgLatency1 = 50.0;
			Double selectivity1 = 0.8;
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long executed2 = 100L;
			Long outputs2 = 70L;
			Double avgLatency2 = 60.0;
			Double selectivity2 = 0.7;
			
			Double selectivity3 = 0.76;
			Double selectivity4 = 0.83;
			
			manager.storeBoltExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity1);
			manager.storeBoltExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity2);
			
			manager.storeBoltExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, executed1, outputs1, avgLatency1, selectivity3);
			manager.storeBoltExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, executed2, outputs2, avgLatency2, selectivity4);
			
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long outputs1 = 100L;
			Long throughput1 = 75L;
			Long losses1 = 10L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long outputs2 = 120L;
			Long throughput2 = 90L;
			Long losses2 = 15L;
			Double avgLatency2 = 700.0;
			
			Long throughput3 = 87L;
			Long throughput4 = 105L;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput1, losses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput2, losses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput3, losses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput4, losses2, avgLatency2);
			
			HashMap<Integer, Long> actualThroughput = manager.getTopologyThroughput(topology, 11, 10);
			
			HashMap<Integer, Long> expectedThroughput = new HashMap<>();
			Long expectedThroughputTimestamp1 = throughput1 + throughput2;
			Long expectedThroughputTimestamp2 = throughput3 + throughput4;
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long outputs1 = 100L;
			Long throughput1 = 75L;
			Long losses1 = 10L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long outputs2 = 120L;
			Long throughput2 = 90L;
			Long losses2 = 15L;
			Double avgLatency2 = 700.0;
			
			Long losses3 = 14L;
			Long losses4 = 16L;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput1, losses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput2, losses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput1, losses3, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput2, losses4, avgLatency2);
			
			HashMap<Integer, Long> actualLosses = manager.getTopologyLosses(topology, 11, 10);
			
			HashMap<Integer, Long> expectedLosses = new HashMap<>();
			Long expectedLossesTimestamp1 = losses1 + losses2;
			Long expectedLossesTimestamp2 = losses3 + losses4;
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
			Integer timestamp1 = 1;
			Integer timestamp2 = 10;
			String topology = "testTopology";
			String component = "testComponent";
			
			String host1 = "testHost1";
			Integer port1 = 0;
			Integer startTask1 = 0;
			Integer endTask1 = 10;
			Long outputs1 = 100L;
			Long throughput1 = 75L;
			Long losses1 = 10L;
			Double avgLatency1 = 500.0;
			
			
			String host2 = "testHost2";
			Integer port2 = 0;
			Integer startTask2 = 11;
			Integer endTask2 = 20;
			Long outputs2 = 120L;
			Long throughput2 = 90L;
			Long losses2 = 15L;
			Double avgLatency2 = 700.0;
			
			Double avgLatency3 = 460.0;
			Double avgLatency4 = 332.0;
			
			manager.storeSpoutExecutorStats(timestamp1, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput1, losses1, avgLatency1);
			manager.storeSpoutExecutorStats(timestamp1, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput2, losses2, avgLatency2);
			
			manager.storeSpoutExecutorStats(timestamp2, host1, port1, topology, component, startTask1, endTask1, outputs1, throughput1, losses1, avgLatency3);
			manager.storeSpoutExecutorStats(timestamp2, host2, port2, topology, component, startTask2, endTask2, outputs2, throughput2, losses2, avgLatency4);
			
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
			StatStorageManager manager = StatStorageManager.getManager("localhost");
		
		Integer timestamp1 = 1;
		Integer timestamp2 = 10;
		Integer timestamp3 = 51;
		Integer timestamp4 = 52; 
		String topology = "testTopology";
		String component = "testComponent";
		
		String host1 = "testHost1";
		Integer port = 0;
		Integer startTask = 11;
		Integer endTask = 20;
		Long outputs1 = 100L;
		Long throughput1 = 75L;
		Long losses1 = 10L;
		Double avgLatency1 = 500.0;
		
		Long outputs2 = 120L;
		Long throughput2 = 90L;
		Long losses2 = 15L;
		Double avgLatency2 = 700.0;
		
		String host2 = "testHost2";
		Long outputs3 = 150L;
		Long throughput3 = 90L;
		Long losses3 = 15L;
		Double avgLatency3 = 700.0;
		
		manager.storeSpoutExecutorStats(timestamp1, host1, port, topology, component, startTask, endTask, outputs1, throughput1, losses1, avgLatency1);
		manager.storeSpoutExecutorStats(timestamp2, host1, port, topology, component, startTask, endTask, outputs2, throughput2, losses2, avgLatency2);
		manager.storeSpoutExecutorStats(timestamp3, host2, port, topology, component, startTask, endTask, outputs3, throughput3, losses3, avgLatency3);
		
		Long actual1 = manager.getFormerValue(component, startTask, endTask, timestamp1, "spout", "outputs");
		Long actual2 = manager.getFormerValue(component, startTask, endTask, timestamp2, "spout", "outputs");
		Long actual3 = manager.getFormerValue(component, startTask, endTask, timestamp3, "spout", "outputs");
		Long actual4 = manager.getFormerValue(component, startTask, endTask, timestamp4, "spout", "outputs");
		Long actual5 = manager.getFormerValue(component, startTask, endTask, timestamp4, "spout", "losses");
		
		assertEquals(0L, actual1, 0);
		assertEquals(100L, actual2, 0);
		assertEquals(120L, actual3, 0);
		assertEquals(150L, actual4, 0);
		assertEquals(15L, actual5, 0);
		
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
	
}
