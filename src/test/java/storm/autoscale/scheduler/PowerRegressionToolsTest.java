/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;
import storm.autoscale.scheduler.regression.PowerRegressionTools;

/**
 * @author Roland
 *
 */
public class PowerRegressionToolsTest extends TestCase {

	private static HashMap<Integer, Long> coordinatesRandom;
	private static HashMap<Integer, Long> coordinatesPower;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Before
	protected void setUp() throws Exception {
		super.setUp();
		coordinatesRandom = new HashMap<>();
		coordinatesRandom.put(1, 0L);
		coordinatesRandom.put(2, 100L);
		coordinatesRandom.put(3, 120L);
		coordinatesRandom.put(4, 40L);
		coordinatesRandom.put(5, 75L);
		coordinatesRandom.put(6, 30L);
		coordinatesRandom.put(7, 90L);
		coordinatesRandom.put(8, 10L);
		
		coordinatesPower = new HashMap<>();
		coordinatesPower.put(1, 5L);
		coordinatesPower.put(2, 6L);
		coordinatesPower.put(3, 12L);
		coordinatesPower.put(4, 9L);
		coordinatesPower.put(5, 21L);
		coordinatesPower.put(6, 33L);
		coordinatesPower.put(7, 45L);
		coordinatesPower.put(8, 59L);
	}

	@Test
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.PowerRegressionTools#linearizedCoordinates(java.util.HashMap)}.
	 */
	public void testLinearizedCoordinates() {
		HashMap<Double, Double> expectedRandom = new HashMap<>();
		expectedRandom.put(Math.log(2.0), 4.605);
		expectedRandom.put(Math.log(3.0), 4.787);
		expectedRandom.put(Math.log(4.0), 3.689);
		expectedRandom.put(Math.log(5.0), 4.317);
		expectedRandom.put(Math.log(6.0), 3.401);
		expectedRandom.put(Math.log(7.0), 4.5);
		expectedRandom.put(Math.log(8.0), 2.303);
		
		HashMap<Double, Double> actualRandom = PowerRegressionTools.linearizeCoordinates(coordinatesRandom);
		
		for(Double x : expectedRandom.keySet()){
			assertEquals(expectedRandom.get(x), actualRandom.get(x), 0.001);
		}
		
		HashMap<Double, Double> expectedPower = new HashMap<>();
		expectedPower.put(Math.log(1.0), 1.609);
		expectedPower.put(Math.log(2.0),1.792);
		expectedPower.put(Math.log(3.0), 2.485);
		expectedPower.put(Math.log(4.0), 2.197);
		expectedPower.put(Math.log(5.0), 3.044);
		expectedPower.put(Math.log(6.0), 3.496);
		expectedPower.put(Math.log(7.0), 3.807);
		expectedPower.put(Math.log(8.0), 4.077);
		
		HashMap<Double, Double> actualExponential = PowerRegressionTools.linearizeCoordinates(coordinatesPower);
		
		for(Double x : expectedPower.keySet()){
			assertEquals(expectedPower.get(x), actualExponential.get(x), 0.001);
		}
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.PowerRegressionTools#regressionCoeff(java.util.HashMap)}.
	 */
	@Test
	public void testRegressionCoeff() {
		assertEquals(285.959, PowerRegressionTools.regressionCoeff(coordinatesRandom), 0.001);
		assertEquals(1.222, PowerRegressionTools.regressionCoeff(coordinatesPower), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.PowerRegressionTools#regressionOffset(java.util.HashMap)}.
	 */
	@Test
	public void testRegressionOffset() {
		assertEquals(-468.664, PowerRegressionTools.regressionOffset(coordinatesRandom), 0.001);
		assertEquals(1.194, PowerRegressionTools.regressionOffset(coordinatesPower), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.PowerRegressionTools#determinationCoeff(java.util.HashMap)}.
	 */
	@Test
	public void testDeterminationCoeff() {
		assertEquals(0.578, PowerRegressionTools.determinationCoeff(coordinatesRandom), 0.001);
		assertEquals(0.85, PowerRegressionTools.determinationCoeff(coordinatesPower), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.PowerRegressionTools#estimateYCoordinate(java.lang.Number, java.util.HashMap)}.
	 */
	@Test
	public void testEstimateYCoordinate() {
		assertEquals(0.0, PowerRegressionTools.estimateYCoordinate(12, coordinatesRandom), 1.0);
		assertEquals(65.9, PowerRegressionTools.estimateYCoordinate(12, coordinatesPower), 1.0);
	}

}
