package cs455.hadoop.Reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Writable.CommonNGramWritable;

public class MostFreqNGramReducer extends
		Reducer<Text, CommonNGramWritable, Text, CommonNGramWritable> {

	/**
	 * This reducer takes in filenames and finds their most commonly occurring
	 * ngram. It writes the following key value pair to the output file:
	 * <filename + ngram, CommonNGramWritable>.
	 */
	public void reduce(Text key, Iterable<CommonNGramWritable> values,
			Context context) throws IOException, InterruptedException {

		int mostFreqNGram = Integer.MIN_VALUE;

		CommonNGramWritable output = new CommonNGramWritable();

		ArrayList<Text> nGrams = new ArrayList<Text>();
		ArrayList<IntWritable> nGramCounts = new ArrayList<IntWritable>();
		ArrayList<Text> decades = new ArrayList<Text>();

		for (CommonNGramWritable i : values) {
			mostFreqNGram = Math.max(mostFreqNGram, i.getnGramCount());
			Text nGram = new Text();
			IntWritable nGramCount = new IntWritable();
			Text decade = new Text();
			nGram.set(i.getnGram());
			nGramCount.set(i.getnGramCount());
			decade.set(i.getDecade());
			nGrams.add(nGram);
			nGramCounts.add(nGramCount);
			decades.add(decade);
		}

		output.setMostFreqNGram(new IntWritable(mostFreqNGram));

		for (int i = 0; i < nGrams.size(); i++) {
			output.setnGramCount(nGramCounts.get(i));
			output.setDecade(decades.get(i));
			context.write(new Text(key.toString() + " " + nGrams.get(i)),
					output);
		}
	}
}
