/**
 * 
 */
package storm.autoscale.scheduler.regression;

import java.util.HashMap;

/**
 * @author Roland
 *
 */
public class LogarithmicRegressionTools {

	public static <T extends Number, U extends Number> HashMap<Double, Double> linearizeCoordinates(HashMap<T, U> coordinates){
		HashMap<Double, Double> result = new HashMap<>();
		for(T x : coordinates.keySet()){
			Double xCoordinate = x.doubleValue();
			Double yCoordinate = coordinates.get(x).doubleValue();
			if(xCoordinate < 1.0){
				xCoordinate = 1.0;//we degrade slightly the estimation but it allows to compute a model
			}
			if(yCoordinate < 1.0){
				yCoordinate = 1.0;//we degrade slightly the estimation but it allows to compute a model
			}
			result.put(1.0 / x.doubleValue(), 1.0 / yCoordinate);
		}
		return result;
	}
	
	public static <T extends Number, U extends Number> Double regressionCoeff(HashMap<T, U> coordinates){
		HashMap<Double, Double> linearized = LogarithmicRegressionTools.linearizeCoordinates(coordinates);
		return LinearRegressionTools.regressionCoeff(linearized);
	}
	
	public static <T extends Number, U extends Number> Double regressionOffset(HashMap<T, U> coordinates){
		HashMap<Double, Double> linearized = LogarithmicRegressionTools.linearizeCoordinates(coordinates);
		return LinearRegressionTools.regressionOffset(linearized);
	}
	
	public static <T extends Number, U extends Number> Double correlationCoeff(HashMap<T, U> coordinates){
		HashMap<Double, Double> linearized = LogarithmicRegressionTools.linearizeCoordinates(coordinates);
		return LinearRegressionTools.correlationCoeff(linearized);
	}
	
	public static <T extends Number, U extends Number> Double estimateYCoordinate(T xCoordinate, HashMap<T, U> coordinates){
		Double regressionCoeff = LogarithmicRegressionTools.regressionCoeff(coordinates);
		Double regressionOffset = LogarithmicRegressionTools.regressionOffset(coordinates);
		return (regressionCoeff * xCoordinate.doubleValue()) / (regressionOffset + xCoordinate.doubleValue());
	}
}