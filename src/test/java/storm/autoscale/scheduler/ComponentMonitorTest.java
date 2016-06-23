/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.TestCase;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class ComponentMonitorTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#isInputDecreasing(java.lang.String)}.
	 */
	public void testIsInputDecreasing() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 50L);
		inputRecords1.put(9, 52L);
		inputRecords1.put(8, 85L);
		inputRecords1.put(7, 106L);
		inputRecords1.put(6, 142L);
		inputRecords1.put(5, 156L);
		inputRecords1.put(4, 191L);
		inputRecords1.put(3, 219L);
		inputRecords1.put(2, 245L);
		inputRecords1.put(1, 270L);
	
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 50L);
		inputRecords2.put(9, 52L);
		inputRecords2.put(8, 85L);
		inputRecords2.put(7, 91L);
		inputRecords2.put(6, 142L);
		inputRecords2.put(5, 156L);
		inputRecords2.put(4, 160L);
		inputRecords2.put(3, 219L);
		inputRecords2.put(2, 235L);
		inputRecords2.put(1, 260L);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		assertEquals(true, cm.isInputDecreasing("component1"));
		assertEquals(false, cm.isInputDecreasing("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#isInputStable(java.lang.String)}.
	 */
	public void testIsInputStable() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 270L);
		inputRecords1.put(9, 245L);
		inputRecords1.put(8, 219L);
		inputRecords1.put(7, 191L);
		inputRecords1.put(6, 156L);
		inputRecords1.put(5, 142L);
		inputRecords1.put(4, 106L);
		inputRecords1.put(3, 85L);
		inputRecords1.put(2, 52L);
		inputRecords1.put(1, 50L);
	
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 220L);
		inputRecords2.put(9, 215L);
		inputRecords2.put(8, 209L);
		inputRecords2.put(7, 198L);
		inputRecords2.put(6, 116L);
		inputRecords2.put(5, 102L);
		inputRecords2.put(4, 91L);
		inputRecords2.put(3, 85L);
		inputRecords2.put(2, 52L);
		inputRecords2.put(1, 50L);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		assertEquals(false, cm.isInputStable("component1"));
		assertEquals(true, cm.isInputStable("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#isInputIncreasing(java.lang.String)}.
	 */
	public void testIsInputIncreasing() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 270L);
		inputRecords1.put(9, 245L);
		inputRecords1.put(8, 219L);
		inputRecords1.put(7, 191L);
		inputRecords1.put(6, 156L);
		inputRecords1.put(5, 142L);
		inputRecords1.put(4, 106L);
		inputRecords1.put(3, 85L);
		inputRecords1.put(2, 52L);
		inputRecords1.put(1, 50L);
	
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 260L);
		inputRecords2.put(9, 235L);
		inputRecords2.put(8, 219L);
		inputRecords2.put(7, 160L);
		inputRecords2.put(6, 156L);
		inputRecords2.put(5, 142L);
		inputRecords2.put(4, 91L);
		inputRecords2.put(3, 85L);
		inputRecords2.put(2, 52L);
		inputRecords2.put(1, 50L);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		assertEquals(true, cm.isInputIncreasing("component1"));
		assertEquals(false, cm.isInputIncreasing("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#isCongested(java.lang.String)}.
	 */
	public void testIsCongested() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 270L);
		inputRecords1.put(9, 245L);
		inputRecords1.put(8, 219L);
		inputRecords1.put(7, 191L);
		inputRecords1.put(6, 156L);
		inputRecords1.put(5, 142L);
		inputRecords1.put(4, 106L);
		inputRecords1.put(3, 85L);
		inputRecords1.put(2, 52L);
		inputRecords1.put(1, 50L);
		
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 50L);
		inputRecords2.put(9, 52L);
		inputRecords2.put(8, 85L);
		inputRecords2.put(7, 106L);
		inputRecords2.put(6, 142L);
		inputRecords2.put(5, 156L);
		inputRecords2.put(4, 191L);
		inputRecords2.put(3, 219L);
		inputRecords2.put(2, 245L);
		inputRecords2.put(1, 270L);
		
		HashMap<Integer, Long> executedRecords1 = new HashMap<>();
		executedRecords1.put(10, 59L);
		executedRecords1.put(9, 61L);
		executedRecords1.put(8, 64L);
		executedRecords1.put(7, 59L);
		executedRecords1.put(6, 62L);
		executedRecords1.put(5, 60L);
		executedRecords1.put(4, 61L);
		executedRecords1.put(3, 58L);
		executedRecords1.put(2, 56L);
		executedRecords1.put(1, 55L);
		
		HashMap<Integer, Long> executedRecords2 = new HashMap<>();
		executedRecords2.put(10, 230L);
		executedRecords2.put(9, 227L);
		executedRecords2.put(8, 232L);
		executedRecords2.put(7, 165L);
		executedRecords2.put(6, 159L);
		executedRecords2.put(5, 143L);
		executedRecords2.put(4, 118L);
		executedRecords2.put(3, 58L);
		executedRecords2.put(2, 56L);
		executedRecords2.put(1, 55L);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, executedRecords1, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords1, executedRecords2, null, null, null);
		ComponentWindowedStats cws3 = new ComponentWindowedStats("component3", inputRecords2, executedRecords1, null, null, null);
		ComponentWindowedStats cws4 = new ComponentWindowedStats("component4", inputRecords2, executedRecords2, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		cm.updateStats(cws3.getId(), cws3);
		cm.updateStats(cws4.getId(), cws4);
		
		assertEquals(true, cm.isCongested("component1"));
		assertEquals(false, cm.isCongested("component2"));
		assertEquals(false, cm.isCongested("component3"));
		assertEquals(false, cm.isCongested("component4"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#couldCongest(java.lang.String)}.
	 */
	public void testCouldCongest() {
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#getCongested()}.
	 */
	public void testGetCongested() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 270L);
		inputRecords1.put(9, 245L);
		inputRecords1.put(8, 219L);
		inputRecords1.put(7, 191L);
		inputRecords1.put(6, 156L);
		inputRecords1.put(5, 142L);
		inputRecords1.put(4, 106L);
		inputRecords1.put(3, 85L);
		inputRecords1.put(2, 52L);
		inputRecords1.put(1, 50L);
		
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 50L);
		inputRecords2.put(9, 52L);
		inputRecords2.put(8, 85L);
		inputRecords2.put(7, 106L);
		inputRecords2.put(6, 142L);
		inputRecords2.put(5, 156L);
		inputRecords2.put(4, 191L);
		inputRecords2.put(3, 219L);
		inputRecords2.put(2, 245L);
		inputRecords2.put(1, 270L);
		
		HashMap<Integer, Long> executedRecords1 = new HashMap<>();
		executedRecords1.put(10, 59L);
		executedRecords1.put(9, 61L);
		executedRecords1.put(8, 64L);
		executedRecords1.put(7, 59L);
		executedRecords1.put(6, 62L);
		executedRecords1.put(5, 60L);
		executedRecords1.put(4, 61L);
		executedRecords1.put(3, 58L);
		executedRecords1.put(2, 56L);
		executedRecords1.put(1, 55L);
		
		HashMap<Integer, Long> executedRecords2 = new HashMap<>();
		executedRecords2.put(10, 230L);
		executedRecords2.put(9, 227L);
		executedRecords2.put(8, 232L);
		executedRecords2.put(7, 165L);
		executedRecords2.put(6, 159L);
		executedRecords2.put(5, 143L);
		executedRecords2.put(4, 118L);
		executedRecords2.put(3, 58L);
		executedRecords2.put(2, 56L);
		executedRecords2.put(1, 55L);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, executedRecords1, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords1, executedRecords2, null, null, null);
		ComponentWindowedStats cws3 = new ComponentWindowedStats("component3", inputRecords2, executedRecords1, null, null, null);
		ComponentWindowedStats cws4 = new ComponentWindowedStats("component4", inputRecords2, executedRecords2, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		cm.updateStats(cws3.getId(), cws3);
		cm.updateStats(cws4.getId(), cws4);
		
		ArrayList<String> expected = new ArrayList<>();
		expected.add("component1");
		assertEquals(expected, cm.getCongested());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#getPotentialCongested()}.
	 */
	public void testGetPotentialCongested() {
	}

}
