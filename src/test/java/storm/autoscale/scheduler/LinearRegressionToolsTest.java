/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.HashMap;

import junit.framework.TestCase;
import storm.autoscale.scheduler.regression.LinearRegressionTools;

/**
 * @author Roland
 *
 */
public class LinearRegressionToolsTest extends TestCase {

	private static HashMap<Integer, Long> coordinatesRandom;
	private static HashMap<Integer, Long> coordinatesLinear;
	private static HashMap<Integer, Long> coordinatesConst;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
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
		
		coordinatesLinear = new HashMap<>();
		coordinatesLinear.put(1, 0L);
		coordinatesLinear.put(2, 15L);
		coordinatesLinear.put(3, 20L);
		coordinatesLinear.put(4, 40L);
		coordinatesLinear.put(5, 56L);
		coordinatesLinear.put(6, 68L);
		coordinatesLinear.put(7, 90L);
		coordinatesLinear.put(8, 110L);
		
		coordinatesConst = new HashMap<>();
		coordinatesConst.put(0, 50L);
		coordinatesConst.put(1, 50L);
		coordinatesConst.put(2, 50L);
		coordinatesConst.put(3, 50L);
		coordinatesConst.put(4, 50L);
		coordinatesConst.put(5, 50L);
		coordinatesConst.put(6, 50L);
		coordinatesConst.put(7, 50L);
		coordinatesConst.put(8, 50L);
		coordinatesConst.put(9, 50L);

	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#nbPoints(java.util.HashMap)}.
	 */
	public void testNbPoints() {
		assertEquals(8.0, LinearRegressionTools.nbPoints(coordinatesRandom), 0);
		assertEquals(8.0, LinearRegressionTools.nbPoints(coordinatesLinear), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#sumXCoordinate(java.util.HashMap)}.
	 */
	public void testSumXCoordinate() {
		assertEquals(36.0, LinearRegressionTools.sumXCoordinate(coordinatesRandom), 0);
		assertEquals(36.0, LinearRegressionTools.sumXCoordinate(coordinatesLinear), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#sumYCoordinate(java.util.HashMap)}.
	 */
	public void testSumYCoordinate() {
		assertEquals(465.0, LinearRegressionTools.sumYCoordinate(coordinatesRandom), 0);
		assertEquals(399.0, LinearRegressionTools.sumYCoordinate(coordinatesLinear), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#sumProdXYCoordinates(java.util.HashMap)}.
	 */
	public void testSumProdXYCoordinates() {
		assertEquals(1985.0, LinearRegressionTools.sumProdXYCoordinates(coordinatesRandom), 0);
		assertEquals(2448.0, LinearRegressionTools.sumProdXYCoordinates(coordinatesLinear), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#sumSqXCoordinates(java.util.HashMap)}.
	 */
	public void testSumSqXCoordinates() {
		assertEquals(204.0, LinearRegressionTools.sumSqXCoordinates(coordinatesRandom), 0);
		assertEquals(204.0, LinearRegressionTools.sumSqXCoordinates(coordinatesLinear), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#avgXCoordinate(java.util.HashMap)}.
	 */
	public void testAvgXCoordinate() {
		assertEquals(4.5, LinearRegressionTools.avgXCoordinate(coordinatesRandom), 0);
		assertEquals(4.5, LinearRegressionTools.avgXCoordinate(coordinatesLinear), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#avgYCoordinate(java.util.HashMap)}.
	 */
	public void testAvgYCoordinate() {
		assertEquals(58.125, LinearRegressionTools.avgYCoordinate(coordinatesRandom), 0);
		assertEquals(49.875, LinearRegressionTools.avgYCoordinate(coordinatesLinear), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#regressionCoeff(java.util.HashMap)}.
	 */
	public void testLinearRegressionCoeff() {
		assertEquals(-2.5595, LinearRegressionTools.regressionCoeff(coordinatesRandom), 0.001);
		assertEquals(15.536, LinearRegressionTools.regressionCoeff(coordinatesLinear), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#regressionOffset(java.util.HashMap)}.
	 */
	public void testLinearRegressionOffset() {
		assertEquals(69.6428, LinearRegressionTools.regressionOffset(coordinatesRandom), 0.001);
		assertEquals(-20.036, LinearRegressionTools.regressionOffset(coordinatesLinear), 0.001);
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#determinationCoeff(java.util.HashMap)}.
	 */
	public void testLinearDeterminationCoeff() {
		assertEquals(0.02, LinearRegressionTools.determinationCoeff(coordinatesRandom), 0.001);
		assertEquals(0.986, LinearRegressionTools.determinationCoeff(coordinatesLinear), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.regression.LinearRegressionTools#estimateYCoordinate(java.lang.Number,java.util.HashMap)}.
	 */
	public void testEstimateYCoordinate() {
		assertEquals(38.928, LinearRegressionTools.estimateYCoordinate(12, coordinatesRandom), 0.001);
		assertEquals(166.393, LinearRegressionTools.estimateYCoordinate(12, coordinatesLinear), 0.001);	
	}
}
