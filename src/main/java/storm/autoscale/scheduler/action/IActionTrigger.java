package storm.autoscale.scheduler.action;

public interface IActionTrigger extends Runnable{

	public void storeAction(String component, Integer currentDegree, Integer newDegree, String actionType);
	
	public boolean isGracePeriod(String component);
	
	
}
