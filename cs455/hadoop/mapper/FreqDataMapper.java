package cs455.hadoop.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Writable.FreqDataWritable;

public class FreqDataMapper extends
		Mapper<LongWritable, Text, Text, FreqDataWritable> {

	private IntWritable one = new IntWritable(1);

	/**
	 * This mapper parses the file outputted by the ngram frequency calculation
	 * job. It writes to the reducer the following key value pair: <ngram,
	 * FreqDataWritable>.
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String filename = "";
		String nGram = "";
		String decade = "";
		IntWritable nGramCount = null;
		DoubleWritable nGramFreq = null;
		IntWritable numOfDocs = null;

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
			if (ctr < numOfElements - 4) {
				nGram = nGram + " " + word;
			}
			if (ctr == numOfElements - 4) {
				nGramCount = new IntWritable(Integer.parseInt(word));
			}
			if (ctr == numOfElements - 3) {
				nGramFreq = new DoubleWritable(Double.parseDouble(word));
			}
			if (ctr == numOfElements - 2) {
				numOfDocs = new IntWritable(Integer.parseInt(word));
			}
			if (ctr == numOfElements - 1) {
				decade = word;
			}
			ctr++;
		}

		FreqDataWritable output = new FreqDataWritable(new Text(filename),
				nGramFreq, nGramCount, numOfDocs, one, new Text(decade));

		context.write(new Text(nGram), output);

	}
}
