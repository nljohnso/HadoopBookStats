package cs455.hadoop.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Writable.TFIDFWritable;

public class TFIDFReducer extends
		Reducer<Text, TFIDFWritable, Text, TFIDFWritable> {

	/**
	 * This reducer takes in a ngram that has a specified book and calculates
	 * their inverse document frequencies and TF-IDF's. It writes the following
	 * key value pair to the output file: <filename, NGramStatsWritable>.
	 */
	public void reduce(Text key, Iterable<TFIDFWritable> values, Context context)
			throws IOException, InterruptedException {

		double invDocFreq = 0;
		double termFreq = 0;
		int nGramCount = 0;
		int numOfDocs = 0;
		int nGramDocFreq = 0;
		double TFIDF = 0;
		Text decade = null;

		TFIDFWritable output = new TFIDFWritable();

		for (TFIDFWritable i : values) {
			nGramCount = i.getnGramCount().get();
			numOfDocs = i.getNumOfDocs().get();
			nGramDocFreq = i.getnGramDocFreq().get();
			termFreq = i.getTermFreq().get();
			invDocFreq = getInverseFrequency(numOfDocs, nGramDocFreq);
			TFIDF = getTFIDF(termFreq, invDocFreq);
			decade = i.getDecade();
		}

		output.setnGramCount(new IntWritable(nGramCount));
		output.setTermFreq(new DoubleWritable(termFreq));
		output.setNumOfDocs(new IntWritable(numOfDocs));
		output.setnGramDocFreq(new IntWritable(nGramDocFreq));
		output.setInvDocFreq(new DoubleWritable(invDocFreq));
		output.setTFIDF(new DoubleWritable(TFIDF));
		output.setDecade(decade);

		String filename = "";
		String nGram = "";

		String line = key.toString();
		StringTokenizer token = new StringTokenizer(line);

		int ctr = 0;
		while (token.hasMoreTokens()) {
			String word = token.nextToken();
			if (ctr == 0) {
				filename = word;
			} else {
				nGram = nGram + " " + word;
			}
			ctr++;
		}

		output.setnGram(new Text(nGram));

		context.write(new Text(filename), output);

	}

	private double getInverseFrequency(int numOfDocs, int nGramDocFreq) {
		double docNum = (double) numOfDocs;
		double docFreqNum = (double) nGramDocFreq;
		double x = docNum / docFreqNum;
		return Math.log10(x);
	}

	private double getTFIDF(double termFreq, double invDocFreq) {
		return termFreq * invDocFreq;
	}
}
