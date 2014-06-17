package cs455.hadoop.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Writable.NGramStatsWritable;

public class NGramStatsReducer extends
		Reducer<Text, NGramStatsWritable, Text, NGramStatsWritable> {
	
	/**
	 * This reducer takes in a ngram that has a specified decade of publication
	 * and outputs the following key value pair to the output file: <decade,
	 * NGramStatsWritable>.
	 */
	public void reduce(Text key, Iterable<NGramStatsWritable> values,
			Context context) throws IOException, InterruptedException {

		double invDocFreq = 0;
		double termFreq = 0;
		int nGramCount = 0;
		double TFIDF = 0;

		NGramStatsWritable output = new NGramStatsWritable();

		int numberOfValues = 0;
		for (NGramStatsWritable i : values) {
			nGramCount += i.getnGramCount().get();
			termFreq += i.getTermFreq().get();
			invDocFreq += i.getInvDocFreq().get();
			TFIDF += i.getTFIDF().get();
			numberOfValues++;
		}

		output.setnGramCount(new IntWritable(nGramCount));
		output.setTermFreq(new DoubleWritable(termFreq / numberOfValues));
		output.setInvDocFreq(new DoubleWritable(invDocFreq / numberOfValues));
		output.setTFIDF(new DoubleWritable(TFIDF / numberOfValues));

		String decade = "";
		String nGram = "";

		String line = key.toString();
		StringTokenizer token = new StringTokenizer(line);

		int ctr = 0;
		while (token.hasMoreTokens()) {
			String word = token.nextToken();
			if (ctr == 0) {
				decade = word;
			} else {
				nGram = nGram + " " + word;
			}
			ctr++;
		}

		output.setnGram(new Text(nGram));

		context.write(new Text(decade), output);

	}
}
