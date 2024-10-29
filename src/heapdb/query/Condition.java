package heapdb.query;

import heapdb.Tuple;

public abstract class Condition {
	
	/**
	 * Evaluate this condition on a tuple.  
	 * Return true if the condition holds of the tuple, and
	 * false if it doesn't.
	 * @param tuple the tuple to evaluate the condition against
	 * @throws IllegalArgumentException for any errors.
	 */
	public abstract Boolean eval(Tuple tuple);

}
