package com.alg.mapred.anagram;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer extends Reducer<Text, Text, Text, Text> {
	private Text outputValue = new Text();

	public void reduce(Text anagramKey, Iterable<Text> anagramValues,
			Context context) throws IOException, InterruptedException {
		StringBuffer output = new StringBuffer();
		for (Text anagram : anagramValues) {
			output.append(anagram.toString() + " ");
		}
		outputValue.set(output.toString());
		context.write(anagramKey, outputValue);
	}

}
