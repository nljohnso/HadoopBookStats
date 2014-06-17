package cs455.hadoop.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Writable.TFIDFWritable;

public class TFIDFMapper extends
		Mapper<LongWritable, Text, Text, TFIDFWritable> {

	private DoubleWritable zero = new DoubleWritable(0);

	/**
	 * This mapper parses the file outputted by the ngram document frequency
	 * calculation job. It writes to the reducer the following key value pair:
	 * <filename + nGram, NGramStatsWritable>.
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String filename = "";
		String nGram = "";
		IntWritable nGramCount = null;
		DoubleWritable nGramFreq = null;
		IntWritable numOfDocs = null;
		IntWritable nGramDocFreq = null;
		Text decade = null;

		// Parse file for values
		int ctr = 0;
		StringTokenizer token = new StringTokenizer(line);
		int numOfElements = token.countTokens();
		while (token.hasMoreTokens()) {
			String word = token.nextToken();
			if (ctr < numOfElements - 6) {
				nGram = nGram + " " + word;
				ctr++;
				continue;
			}
			if (ctr == numOfElements - 6) {
				filename = word;
			}
			if (ctr == numOfElements - 5) {
				nGramCount = new IntWritable(Integer.parseInt(word));
			}
			if (ctr == numOfElements - 4) {
				nGramFreq = new DoubleWritable(Double.parseDouble(word));
			}
			if (ctr == numOfElements - 3) {
				numOfDocs = new IntWritable(Integer.parseInt(word));
			}
			if (ctr == numOfElements - 2) {
				nGramDocFreq = new IntWritable(Integer.parseInt(word));
			}
			if (ctr == numOfElements - 1) {
				decade = new Text(word);
			}
			ctr++;
		}

		TFIDFWritable output = new TFIDFWritable(nGramFreq, nGramCount,
				numOfDocs, nGramDocFreq, zero, zero, new Text(""), decade);

		context.write(new Text(filename + " " + nGram), output);

	}
}
