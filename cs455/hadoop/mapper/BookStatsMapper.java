package cs455.hadoop.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Writable.BookStatsWritable;

public class BookStatsMapper extends
		Mapper<LongWritable, Text, Text, BookStatsWritable> {

	private DoubleWritable zeroDouble = new DoubleWritable(0);
	private IntWritable zero = new IntWritable(0);
	private IntWritable one = new IntWritable(1);

	/**
	 * This mapper parses each block of data given to it and writes to the
	 * reducer the following key value pair: <filename, BookStatsWritable>.
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Get filename
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		String line = value.toString();

		// Parse the book and get the number of words, sentences, and syllables
		// in each one.
		StringTokenizer token = new StringTokenizer(line);
		while (token.hasMoreTokens()) {
			String word = token.nextToken();
			if (word.contains(".") || word.contains("!") || word.contains("?")) {
				IntWritable syllables = new IntWritable(CountSyllables(word));
				BookStatsWritable bookInfo = new BookStatsWritable(one, one,
						syllables, zeroDouble, zeroDouble);
				context.write(new Text(filename), bookInfo);
			} else {
				IntWritable syllables = new IntWritable(CountSyllables(word));
				BookStatsWritable bookInfo = new BookStatsWritable(one, zero,
						syllables, zeroDouble, zeroDouble);
				context.write(new Text(filename), bookInfo);
			}
		}
	}

	private int CountSyllables(String word) {
		char[] vowels = { 'a', 'e', 'i', 'o', 'u', 'y' };
		String currentWord = word;
		int numVowels = 0;
		boolean lastWasVowel = false;
		for (int i = 0; i < word.length(); i++) {
			char wc = word.charAt(i);
			boolean foundVowel = false;
			for (int j = 0; j < vowels.length; j++) {
				char v = vowels[j];
				// don't count diphthongs
				if (v == wc && lastWasVowel) {
					foundVowel = true;
					lastWasVowel = true;
					break;
				} else if (v == wc && !lastWasVowel) {
					numVowels++;
					foundVowel = true;
					lastWasVowel = true;
					break;
				}
			}

			// if full cycle and no vowel found, set lastWasVowel to false;
			if (!foundVowel)
				lastWasVowel = false;
		}
		// remove es, it's _usually? silent
		if (currentWord.length() > 2
				&& currentWord.substring(currentWord.length() - 2) == "es")
			numVowels--;
		// remove silent e
		else if (currentWord.length() > 1
				&& currentWord.substring(currentWord.length() - 1) == "e")
			numVowels--;

		return numVowels;
	}
}
