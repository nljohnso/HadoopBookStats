package cs455.hadoop.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Writable.TermFreqWritable;

public class TermFreqReducer extends
		Reducer<Text, TermFreqWritable, Text, TermFreqWritable> {
	/**
	 * This reducer takes in ngrams that have a specified book and calculates
	 * it's term frequency for that book. It writes the following key value pair
	 * to the output file: <filename + ngram, TermFreqWritable>.
	 */
	public void reduce(Text key, Iterable<TermFreqWritable> values,
			Context context) throws IOException, InterruptedException {

		double termFreq = 0;

		int nGramCount = 0;
		int mostFreqNGram = 0;
		int numOfDocs = 0;
		Text decade = null;

		TermFreqWritable output = new TermFreqWritable();

		for (TermFreqWritable i : values) {
			nGramCount = i.getnGramCount().get();
			mostFreqNGram = i.getMostFreqNGram().get();
			termFreq = getTermFrequency(nGramCount, mostFreqNGram);
			numOfDocs = i.getNumOfDocs().get();
			decade = i.getDecade();
		}

		output.setnGramCount(new IntWritable(nGramCount));
		output.setTermFreq(new DoubleWritable(termFreq));
		output.setMostFreqNGram(new IntWritable(mostFreqNGram));
		output.setNumOfDocs(new IntWritable(numOfDocs));
		output.setDecade(decade);

		context.write(key, output);

	}

	private double getTermFrequency(int nGramFreq, int mostFreqNGram) {

		int tmp = mostFreqNGram;
		if (mostFreqNGram == 0) {
			tmp = 1;
		}
		return (.5 + ((.5 * (double) nGramFreq) / (double) tmp));
	}
}
