package cs455.hadoop.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Writable.NGramStatsWritable;

public class NGramStatsMapper extends
		Mapper<LongWritable, Text, Text, NGramStatsWritable> {

	/**
	 * This is an optional mapper that parses the file outputted by the TFIDF
	 * calculation job. It writes to the reducer the following key value pair:
	 * <decade + nGram, NGramStatsWritable>.
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String nGram = "";
		IntWritable nGramCount = null;
		DoubleWritable nGramFreq = null;
		DoubleWritable invDocFreq = null;
		DoubleWritable TFIDF = null;
		Text decade = null;

		// Parse the file for values
		int ctr = 0;
		StringTokenizer token = new StringTokenizer(line);
		int numOfElements = token.countTokens();
		while (token.hasMoreTokens()) {
			String word = token.nextToken();
			if (ctr == 0) {
				ctr++;
				continue;
			}
			if (ctr < numOfElements - 5) {
				nGram = nGram + " " + word;
				ctr++;
				continue;
			}
			if (ctr == numOfElements - 5) {
				nGramCount = new IntWritable(Integer.parseInt(word));
			}
			if (ctr == numOfElements - 4) {
				decade = new Text(word);
			}
			if (ctr == numOfElements - 3) {
				nGramFreq = new DoubleWritable(Double.parseDouble(word));
			}
			if (ctr == numOfElements - 2) {
				invDocFreq = new DoubleWritable(Double.parseDouble(word));
			}
			if (ctr == numOfElements - 1) {
				TFIDF = new DoubleWritable(Double.parseDouble(word));
			}
			ctr++;
		}

		NGramStatsWritable output = new NGramStatsWritable(nGramFreq,
				nGramCount, invDocFreq, TFIDF, new Text(nGram), decade);

		context.write(new Text(decade + " " + nGram), output);

	}
}
