package cs455.hadoop.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Writable.TermFreqWritable;

public class TermFreqMapper extends
		Mapper<LongWritable, Text, Text, TermFreqWritable> {

	private DoubleWritable zero = new DoubleWritable(0);

	/**
	 * This mapper parses the file outputted by the most frequent ngram
	 * calculation job. It writes to the reducer the following key value pair:
	 * <filename + ngram, TermFreqWritable>.
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		int numberOfFilesInCorpus = 10;

		String line = value.toString();
		String filename = "";
		String nGram = "";
		String decade = "";
		IntWritable nGramCount = null;
		IntWritable mostFreqNGram = null;

		// Parse the file for values
		int ctr = 0;
		StringTokenizer token = new StringTokenizer(line);
		int numOfElements = token.countTokens();
		while (token.hasMoreTokens()) {
			String word = token.nextToken();
			if (ctr == 0) {
				filename = word;
				ctr++;
				continue;
			}
			if (ctr < numOfElements - 3) {
				nGram = nGram + " " + word;
			}
			if (ctr == numOfElements - 3) {
				nGramCount = new IntWritable(Integer.parseInt(word));
			}
			if (ctr == numOfElements - 2) {
				mostFreqNGram = new IntWritable(Integer.parseInt(word));
			}
			if (ctr == numOfElements - 1) {
				decade = word;
			}
			ctr++;
		}

		TermFreqWritable output = new TermFreqWritable(zero, nGramCount,
				mostFreqNGram, new IntWritable(numberOfFilesInCorpus),
				new Text(decade));

		context.write(new Text(filename + " " + nGram), output);

	}
}
