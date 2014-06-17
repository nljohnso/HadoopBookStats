package cs455.hadoop.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * This class implements Writable and contains word count, sentence count,
 * syllable count, flesch reading ease, and flesch grade level.
 * 
 * @author nljohnso
 * 
 */
public class BookStatsWritable implements Writable {
	private IntWritable wordCount;
	private IntWritable sentenceCount;
	private IntWritable syllableCount;
	private DoubleWritable fleschReadingEase;
	private DoubleWritable fleschKincaidGradeLevel;

	public BookStatsWritable() {
		this.wordCount = new IntWritable();
		this.sentenceCount = new IntWritable();
		this.syllableCount = new IntWritable();
		this.fleschReadingEase = new DoubleWritable();
		this.fleschKincaidGradeLevel = new DoubleWritable();

	}

	public BookStatsWritable(IntWritable wordCount, IntWritable sentenceCount,
			IntWritable syllableCount, DoubleWritable FRE, DoubleWritable FKGL) {
		this.wordCount = wordCount;
		this.sentenceCount = sentenceCount;
		this.syllableCount = syllableCount;
		this.fleschReadingEase = FRE;
		this.fleschKincaidGradeLevel = FKGL;

	}

	public void write(DataOutput dataOutput) throws IOException {
		wordCount.write(dataOutput);
		sentenceCount.write(dataOutput);
		syllableCount.write(dataOutput);
		fleschReadingEase.write(dataOutput);
		fleschKincaidGradeLevel.write(dataOutput);

	}

	public void readFields(DataInput dataInput) throws IOException {
		wordCount.readFields(dataInput);
		sentenceCount.readFields(dataInput);
		syllableCount.readFields(dataInput);
		fleschReadingEase.readFields(dataInput);
		fleschKincaidGradeLevel.readFields(dataInput);
	}

	public IntWritable getWordCount() {
		return wordCount;
	}

	public void setWordCount(IntWritable wordCount) {
		this.wordCount = wordCount;
	}

	public IntWritable getSentenceCount() {
		return sentenceCount;
	}

	public void setSentenceCount(IntWritable sentenceCount) {
		this.sentenceCount = sentenceCount;
	}

	public IntWritable getSyllableCount() {
		return syllableCount;
	}

	public void setSyllableCount(IntWritable syllableCount) {
		this.syllableCount = syllableCount;
	}

	public DoubleWritable getFleschReadingEase() {
		return fleschReadingEase;
	}

	public void setFleschReadingEase(DoubleWritable fleschReadingEase) {
		this.fleschReadingEase = fleschReadingEase;
	}

	public DoubleWritable getFleschKincaidGradeLevel() {
		return fleschKincaidGradeLevel;
	}

	public void setFleschKincaidGradeLevel(
			DoubleWritable fleschKincaidGradeLevel) {
		this.fleschKincaidGradeLevel = fleschKincaidGradeLevel;
	}

	@Override
	public int hashCode() {
		return 0;
		// This is used by HashPartitioner, so implement it if you need it.
	}

	@Override
	public String toString() {
		return "\n\tWord Count: " + wordCount + "\n\tSentence Count: "
				+ sentenceCount + "\n\tSyllable Count " + syllableCount
				+ "\n\tReading Ease " + fleschReadingEase + "\n\tGrade Level "
				+ fleschKincaidGradeLevel;
	}
} // end class
