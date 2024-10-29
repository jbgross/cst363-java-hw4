package heapdb.query;


import heapdb.ITable;
import heapdb.Schema;
import heapdb.Table;
import heapdb.Tuple;

import java.util.ArrayList;

/**
 * A simple select query of the form:
 * select column, column, . . . from table where condition
 * 
 * @author Glenn
 *
 */

public class SelectQuery  {
	
	private Condition cond;
	private String[] colNames;	   // a value of null means return all columns of the table
	
	/**
	 * A query that contains both a where condition and a projection of columns
	 * @param colNames are the columns to return
	 * @param cond is the where clause
	 */
	public SelectQuery(String[] colNames, Condition cond) {
		this.colNames = colNames;
		this.cond = cond;
	}
	
	/**
	 * A query that contains both a where condition.  All columns 
	 * of the Tuples are returned.
	 * @param cond is the where clause
	 */
	public SelectQuery(Condition cond) {
		this(null, cond);
	}
	
	
	public static Table naturalJoin(ITable table1, ITable table2) {
		Schema resultSchema = table1.getSchema().naturaljoin(table2.getSchema());
		Table result = new Table(resultSchema);
		ArrayList<String> joinColumns  = new ArrayList<>();
		// TODO  find the list of join column of the 2 tables
		Schema s1 = table1.getSchema();
		Schema s2 = table2.getSchema();
		for(int i = 0; i < s1.size(); i++) {
			for(int k = 0; k < s2.size(); k++) {
				if(s1.getName(i).equals(s2.getName(k))) {
					joinColumns.add(s1.getName(i));
					break;
				}
			}
		}
		for (Tuple t1: table1) {
			for (Tuple t2: table2) {
				boolean isMatch = true;
				for(String column : joinColumns) {
					if(! t1.get(column).equals(t2.get(column))) {
						isMatch = false;
						break;
					}
				}
				if(isMatch) {
					Tuple joinTuple = Tuple.joinTuple(resultSchema, t1, t2);
					result.insert(joinTuple);
				}
			}
		}
		return result;
	}
	
	public ITable eval(ITable table) {
		Schema s = table.getSchema();
		boolean newSchema = false;
		// if there is a projection operation, make the new schema
		if (colNames != null) {
			s = table.getSchema().project(colNames);
			newSchema = true;
		}
		
		Table result = new Table(s);
		for (Tuple t: table) { 
			// if tuple t satisfies the condition, insert t into the result table.
			if (cond.eval(t)) {
				result.insert(t.project(s));
			}
		}
		return result;
	}

	@Override
	public String toString() {
	    String proj_columns;
	    if (colNames != null) {
	    	proj_columns = String.join(",", colNames);
	    } else {
	    	proj_columns = "*";
	    }
	    return "select " + proj_columns + " where " + cond;
	}

}
