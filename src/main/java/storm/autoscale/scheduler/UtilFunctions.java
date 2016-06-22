/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author Roland
 *
 */
public class UtilFunctions {

	public static ArrayList<ArrayList<Integer>> getBuckets(ArrayList<Integer> values, Integer nbBuckets){
		ArrayList<ArrayList<Integer>> result = new ArrayList<>();
		int nbTasks = values.size();
		int quotient = nbTasks / nbBuckets;
		int remainder = nbTasks - (nbBuckets * quotient);
		if(remainder == 0){
			for(int i = 0; i < nbTasks; i += quotient){
				ArrayList<Integer> delimiters = new ArrayList<>();
				int start = values.get(i);
				int end = values.get(i + quotient - 1);
				delimiters.add(start);
				delimiters.add(end);
				result.add(delimiters);
			}
		}else{
			for(int i = 0; i < nbTasks - (quotient + remainder); i += quotient){
				ArrayList<Integer> delimiters = new ArrayList<>();
				int start = values.get(i);
				int end = values.get(i + quotient - 1);
				delimiters.add(start);
				delimiters.add(end);
				result.add(delimiters);
			}
			ArrayList<Integer> delimiters = new ArrayList<>();
			int start = values.get(nbTasks - (quotient + remainder));
			int end = values.get(nbTasks - 1);
			delimiters.add(start);
			delimiters.add(end);
			result.add(delimiters);
		}
		return result;
	}
	
	public static class DecreasingIntOrder implements Comparator<Integer>{

		@Override
		public int compare(Integer o1, Integer o2) {
			if(o1.intValue() < o2.intValue()){
				return 1;
			}
			return -1;
		}
		
	}
	
	public static class DecreasingDoubleOrder implements Comparator<Double>{

		@Override
		public int compare(Double o1, Double o2) {
			if(o1.doubleValue() < o2.doubleValue()){
				return 1;
			}
			return -1;
		}
		
	}
}