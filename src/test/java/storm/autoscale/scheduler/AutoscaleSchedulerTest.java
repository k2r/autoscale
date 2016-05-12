/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.ArrayList;

import junit.framework.TestCase;

/**
 * @author Roland
 *
 */
public class AutoscaleSchedulerTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.AutoscaleScheduler#getBorderTasks(java.util.ArrayList, java.lang.Integer)}.
	 */
	public final void testGetBorderTasks() {
		ArrayList<Integer> tasks = new ArrayList<>();
		tasks.add(7);
		tasks.add(8);
		tasks.add(9);
		tasks.add(10);
		tasks.add(11);
		tasks.add(12);
		tasks.add(13);
		
		ArrayList<ArrayList<Integer>> expectedBorder1 = new ArrayList<>();
		ArrayList<Integer> delimiters11 = new ArrayList<>();
		delimiters11.add(7);
		delimiters11.add(13);
		expectedBorder1.add(delimiters11);
		
		ArrayList<ArrayList<Integer>> expectedBorder2 = new ArrayList<>();
		ArrayList<Integer> delimiters21 = new ArrayList<>();
		delimiters21.add(7);
		delimiters21.add(9);
		
		ArrayList<Integer> delimiters22 = new ArrayList<>();
		delimiters22.add(10);
		delimiters22.add(13);
		
		expectedBorder2.add(delimiters21);
		expectedBorder2.add(delimiters22);
		
		ArrayList<ArrayList<Integer>> expectedBorder3 = new ArrayList<>();
		ArrayList<Integer> delimiters31 = new ArrayList<>();
		delimiters31.add(7);
		delimiters31.add(8);
		
		ArrayList<Integer> delimiters32 = new ArrayList<>();
		delimiters32.add(9);
		delimiters32.add(10);
		
		ArrayList<Integer> delimiters33 = new ArrayList<>();
		delimiters33.add(11);
		delimiters33.add(13);
		
		expectedBorder3.add(delimiters31);
		expectedBorder3.add(delimiters32);
		expectedBorder3.add(delimiters33);
		
		ArrayList<ArrayList<Integer>> expectedBorder7 = new ArrayList<>();
		ArrayList<Integer> delimiters71 = new ArrayList<>();
		delimiters71.add(7);
		delimiters71.add(7);
		ArrayList<Integer> delimiters72 = new ArrayList<>();
		delimiters72.add(8);
		delimiters72.add(8);
		ArrayList<Integer> delimiters73 = new ArrayList<>();
		delimiters73.add(9);
		delimiters73.add(9);
		ArrayList<Integer> delimiters74 = new ArrayList<>();
		delimiters74.add(10);
		delimiters74.add(10);
		ArrayList<Integer> delimiters75 = new ArrayList<>();
		delimiters75.add(11);
		delimiters75.add(11);
		ArrayList<Integer> delimiters76 = new ArrayList<>();
		delimiters76.add(12);
		delimiters76.add(12);
		ArrayList<Integer> delimiters77 = new ArrayList<>();
		delimiters77.add(13);
		delimiters77.add(13);
		
		expectedBorder7.add(delimiters71);
		expectedBorder7.add(delimiters72);
		expectedBorder7.add(delimiters73);
		expectedBorder7.add(delimiters74);
		expectedBorder7.add(delimiters75);
		expectedBorder7.add(delimiters76);
		expectedBorder7.add(delimiters77);
		
		AutoscaleScheduler scheduler = new AutoscaleScheduler();
		
		ArrayList<ArrayList<Integer>> borders1 = scheduler.getBorderTasks(tasks, 1);
		//ArrayList<ArrayList<Integer>> borders2 = scheduler.getBorderTasks(tasks, 2);
		ArrayList<ArrayList<Integer>> borders3 = scheduler.getBorderTasks(tasks, 3);
		ArrayList<ArrayList<Integer>> borders7 = scheduler.getBorderTasks(tasks, 7);
		
		assertEquals(expectedBorder1, borders1);
		//assertEquals(expectedBorder2, borders2);
		assertEquals(expectedBorder3, borders3);
		assertEquals(expectedBorder7, borders7);
	}
}