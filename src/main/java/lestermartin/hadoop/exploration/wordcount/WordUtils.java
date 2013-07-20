package lestermartin.hadoop.exploration.wordcount;

import java.util.ArrayList;
import java.util.List;


public class WordUtils {
	
	/**
	 * Uses regular expressions to split the line up by non-word characters.
	 */
	static public List<String> splitWords(String lineToSplit) {
		List<String> words = new ArrayList<String>();

		for (String word : lineToSplit.split("\\W+")) {
			if (word.length() > 0) {
				words.add(word);
			}
		}
		return words;
	}

}
