/**
 * 
 */
package storm.autoscale.scheduler.actions;

import java.util.ArrayList;

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
}
