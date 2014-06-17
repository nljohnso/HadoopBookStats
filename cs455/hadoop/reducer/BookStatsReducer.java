package cs455.hadoop.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Writable.BookStatsWritable;

public class BookStatsReducer extends
		Reducer<Text, BookStatsWritable, Text, BookStatsWritable> {

	/**
	 * This reducer takes in filenames and sums up the total number of words,
	 * sentences, and syllables and then calculates the Flesch reading ease and
	 * grade level. It writes the following key value pair to the output file:
	 * <filename, BookStatsWritable>.
	 */
	public void reduce(Text key, Iterable<BookStatsWritable> values,
			Context context) throws IOException, InterruptedException {

		int totalWords = 0;
		int totalSentences = 0;
		int totalSyllables = 0;
		double fleschReadingEase = 0;
		double fleschKincaidGradeLevel = 0;

		BookStatsWritable output = new BookStatsWritable();

		for (BookStatsWritable current : values) {
			totalWords += current.getWordCount().get();
			totalSentences += current.getSentenceCount().get();
			totalSyllables += current.getSyllableCount().get();
			fleschReadingEase = FRE(totalWords, totalSentences, totalSyllables);
			fleschKincaidGradeLevel = FKGL(totalWords, totalSentences,
					totalSyllables);
		}
		output.setWordCount(new IntWritable(totalWords));
		output.setSentenceCount(new IntWritable(totalSentences));
		output.setSyllableCount(new IntWritable(totalSyllables));
		output.setFleschReadingEase(new DoubleWritable(fleschReadingEase));
		output.setFleschKincaidGradeLevel(new DoubleWritable(
				fleschKincaidGradeLevel));

		context.write(key, output);
	}

	private double FKGL(int wordCount, int sentenceCount, int syllableCount) {

		if (wordCount == 0 || sentenceCount == 0 || syllableCount == 0) {
			return 0;
		}

		double tmp = (double) wordCount / sentenceCount;
		double tmp2 = (double) syllableCount / wordCount;
		double x = (.39 * tmp);
		double y = (11.8 * tmp2);

		return (x + y - 15.59);
	}

	private double FRE(int wordCount, int sentenceCount, int syllableCount) {

		if (wordCount == 0 || sentenceCount == 0 || syllableCount == 0) {
			return 0;
		}

		double tmp = (double) wordCount / sentenceCount;
		double tmp2 = (double) syllableCount / wordCount;
		double x = (1.015 * tmp);
		double y = (84.6 * tmp2);

		return (206.835 - x - y);
	}
}

// public void reduce(Text key, Iterable<TestWritable> values, Context context)
// throws IOException, InterruptedException {
//
// // int sum = 0;
// // int maxSum = Integer.MIN_VALUE;
//
// String name = "";
// String info = "";
//
// TestWritable output = new TestWritable();
//
// for (TestWritable value: values) {
// // sum += value.get();
// // maxSum = Math.max(maxSum, value.get());
// name = value.getBookName().toString();
// info = value.getBookContents().toString();
//
// }
//
// output.setBookName(new Text(name));
// output.setBookContents(new Text(info));
//
// context.write(key, output);
// }
// }
