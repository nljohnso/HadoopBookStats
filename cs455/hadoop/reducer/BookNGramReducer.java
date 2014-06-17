package cs455.hadoop.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Writable.NGramWritable;

public class BookNGramReducer extends
		Reducer<Text, NGramWritable, Text, NGramWritable> {

	/**
	 * This reducer takes in ngrams that have a specified book and sums up the
	 * total number of like-ngrams for that book. It writes the following key
	 * value pair to the output file: <filename + ngram, NGramWritable>.
	 */
	public void reduce(Text key, Iterable<NGramWritable> values, Context context)
			throws IOException, InterruptedException {

		int nGramCount = 0;
		Text decade = null;

		for (NGramWritable i : values) {
			nGramCount += i.getnGramCount().get();
			decade = i.getDecade();
		}

		context.write(key, new NGramWritable(new IntWritable(nGramCount),
				decade));
	}

}
