package cs455.hadoop.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import cs455.hadoop.Parser.Sentence;
import cs455.hadoop.Writable.NGramWritable;

public class BookNGramMapper extends
		Mapper<LongWritable, Text, Text, NGramWritable> {

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private int nGramNum = 1;

	/**
	 * This mapper also parses the file for a specified ngram value. It writes
	 * to the reducer the following key value pair: <filename + ngram,
	 * NGramWritable>.
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Get filename
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String filename = fileSplit.getPath().getName();

		String year = "";

		// Get the year from the filename
		boolean start = false;
		for (int i = 0; i < filename.length(); i++) {
			if (filename.charAt(i) == '.') {
				break;
			}
			if (filename.charAt(i) == 'r') {
				start = true;
				continue;
			}
			if (start) {
				year = year + filename.charAt(i);
			}
		}

		boolean BC = false;

		if (year.contains("BC")) {
			BC = true;
		}

		int fromDecade = 0;
		int toDecade = 0;
		int intYear = 0;

		if (BC) {
			intYear = 0 - Integer.parseInt(year.substring(0, 3));
		} else {
			intYear = Integer.parseInt(year);
		}

		// Set the decade span
		fromDecade = (intYear - (intYear % 10)) + 1;
		toDecade = fromDecade + 9;

		if (intYear % 10 == 0) {
			toDecade = intYear;
			fromDecade = intYear - 9;
		}

		String decade = new String(fromDecade + "-" + toDecade);

		String line = value.toString();

		// Parse the ngrams from the book
		List<String> nGrams = new ArrayList<String>();
		String lineParse = line.toLowerCase()
				.replaceAll("[-+.^:,()?'!;\"]", "");
		Sentence lineSentence = new Sentence(lineParse);

		nGrams = lineSentence.wordNGrams(lineSentence, nGramNum);

		for (String aNGram : nGrams) {
			word.set(aNGram);
			context.write(new Text(filename + " " + word), new NGramWritable(
					one, new Text(decade)));
		}
	}
}
