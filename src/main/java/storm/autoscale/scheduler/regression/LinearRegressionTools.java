/**
 * 
 */
package storm.autoscale.scheduler.regression;

import java.util.HashMap;

/**
 * @author Roland
 *
 */
public class LinearRegressionTools{
	
	public static <T extends Number, U extends Number> Integer nbPoints(HashMap<T, U> coordinates){
		return coordinates.size();
	}
	
	public static <T extends Number, U extends Number> Double sumXCoordinate(HashMap<T, U> coordinates){
		Double sum = 0.0;
		for(T xCoordinate : coordinates.keySet()){
			sum += xCoordinate.doubleValue();
		}	
		return sum;
	}
	
	public static <T extends Number, U extends Number> Double sumYCoordinate(HashMap<T, U> coordinates){
		Double sum = 0.0;
		for(U yCoordinate : coordinates.values()){
			sum += yCoordinate.doubleValue();
		}	
		return sum;
	}
	
	public static <T extends Number, U extends Number> Double sumProdXYCoordinates(HashMap<T, U> coordinates){
		Double sum = 0.0;
		for(T xCoordinate : coordinates.keySet()){
			U yCoordinate = coordinates.get(xCoordinate);
			Double product = xCoordinate.doubleValue() * yCoordinate.doubleValue();
			sum += product;
		}
		return sum;
	}
	
	public static <T extends Number, U extends Number> Double sumSqXCoordinates(HashMap<T, U> coordinates){
		Double sum = 0.0;
		for(T xCoordinate : coordinates.keySet()){
			Double product = xCoordinate.doubleValue() * xCoordinate.doubleValue();
			sum += product;
		}
		return sum;
	}
	
	public static <T extends Number, U extends Number> Double avgXCoordinate(HashMap<T, U> coordinates){
		return LinearRegressionTools.sumXCoordinate(coordinates) / LinearRegressionTools.nbPoints(coordinates);
	}
	
	public static <T extends Number, U extends Number> Double avgYCoordinate(HashMap<T, U> coordinates){
		return LinearRegressionTools.sumYCoordinate(coordinates) / LinearRegressionTools.nbPoints(coordinates);
	}
	
	public static <T extends Number, U extends Number> Double regressionCoeff(HashMap<T, U> coordinates){
		Double dividend = (LinearRegressionTools.nbPoints(coordinates) * LinearRegressionTools.sumProdXYCoordinates(coordinates)) 
				- (LinearRegressionTools.sumXCoordinate(coordinates) * LinearRegressionTools.sumYCoordinate(coordinates));
		Double divisor = (LinearRegressionTools.nbPoints(coordinates) * LinearRegressionTools.sumSqXCoordinates(coordinates))
				- (Math.pow(LinearRegressionTools.sumXCoordinate(coordinates), 2));
		return dividend / divisor;
	}
	
	public static <T extends Number, U extends Number> Double regressionOffset(HashMap<T, U> coordinates){
		return LinearRegressionTools.avgYCoordinate(coordinates) - (LinearRegressionTools.regressionCoeff(coordinates) * LinearRegressionTools.avgXCoordinate(coordinates));
	}
	
	public static <T extends Number, U extends Number> Double correlationCoeff(HashMap<T, U> coordinates){
		Double avgXCoordinate = LinearRegressionTools.avgXCoordinate(coordinates);
		Double avgYCoordinate = LinearRegressionTools.avgYCoordinate(coordinates);
		
		Double derivXYCoordinate = 0.0;
		Double derivSqrXCoordinate = 0.0;
		Double derivSqrYCoordinate = 0.0;
		
		for(T xCoordinate : coordinates.keySet()){
			U yCoordinate = coordinates.get(xCoordinate);
			derivXYCoordinate += (xCoordinate.doubleValue() - avgXCoordinate) * (yCoordinate.doubleValue() - avgYCoordinate);
			derivSqrXCoordinate += Math.pow(xCoordinate.doubleValue() - avgXCoordinate, 2);
			derivSqrYCoordinate += Math.pow(yCoordinate.doubleValue() - avgYCoordinate, 2);
		}
		
		return derivXYCoordinate / (Math.sqrt(derivSqrXCoordinate * derivSqrYCoordinate));
	}
	
	public static <T extends Number, U extends Number> Double estimateYCoordinate(T xCoordinate, HashMap<T, U> coordinates){
		Double regressionCoeff = LinearRegressionTools.regressionCoeff(coordinates);
		Double regressionOffset = LinearRegressionTools.regressionOffset(coordinates);
		return (regressionCoeff * xCoordinate.doubleValue()) + regressionOffset;
	}
}