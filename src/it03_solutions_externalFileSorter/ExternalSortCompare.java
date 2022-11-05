package it03_solutions_externalFileSorter;

import java.util.Comparator;

public class ExternalSortCompare implements Comparator<String> {
	@Override
	public int compare(String s1, String s2) {
		//TODO check for string length > 10
		return s1.substring(0, 10).compareTo(s2.substring(0, 10));
	}
}
