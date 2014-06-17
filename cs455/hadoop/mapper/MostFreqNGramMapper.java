package cs455.hadoop.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Writable.CommonNGramWritable;

public class MostFreqNGramMapper extends
		Mapper<LongWritable, Text, Text, CommonNGramWritable> {

	/**
	 * This mapper parses the file outputted by the ngram parse job. It writes
	 * to the reducer the following key value pair: <filename,
	 * CommonNGramWritable>.
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String nGramCountString = "";
		String filename = "";
		String nGram = "";
		String decade = "";

		// Parse the file for values
		StringTokenizer token = new StringTokenizer(line);
		int ctr = 0;
		int numOfElements = token.countTokens();
		while (token.hasMoreTokens()) {
			String word = token.nextToken();
			if (ctr == 0) {
				filename = word;
				ctr++;
				continue;
			}
			if (ctr < numOfElements - 2) {
				nGram = nGram + " " + word;
				ctr++;
				continue;
			}
			if (ctr == numOfElements - 2) {
				nGramCountString = word.trim();
			}
			if (ctr == numOfElements - 1) {
				decade = word;
			}
			ctr++;
		}

		int nGramCount = Integer.parseInt(nGramCountString);

		CommonNGramWritable output = new CommonNGramWritable(new Text(nGram),
				new IntWritable(nGramCount), new IntWritable(0), new Text(
						decade));

		context.write(new Text(filename), output);

	}
}
