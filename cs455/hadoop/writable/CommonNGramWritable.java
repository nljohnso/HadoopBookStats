package cs455.hadoop.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class implements Writable and contains ngram, ngram count, most frequent
 * ngram, and decade span.
 * 
 * @author nljohnso
 * 
 */
public class CommonNGramWritable implements Writable {

	private Text nGram;
	private IntWritable nGramCount;
	private IntWritable mostFreqNGram;
	private Text decade;

	public CommonNGramWritable() {
		this.nGram = new Text();
		this.nGramCount = new IntWritable();
		this.mostFreqNGram = new IntWritable();
		this.decade = new Text();
	}

	public CommonNGramWritable(Text nGram, IntWritable nGramCount,
			IntWritable mostFreqNGram, Text decade) {
		this.nGram = nGram;
		this.nGramCount = nGramCount;
		this.mostFreqNGram = mostFreqNGram;
		this.decade = decade;
	}

	public void write(DataOutput dataOutput) throws IOException {
		nGram.write(dataOutput);
		nGramCount.write(dataOutput);
		mostFreqNGram.write(dataOutput);
		decade.write(dataOutput);
	}

	public void readFields(DataInput dataInput) throws IOException {
		nGram.readFields(dataInput);
		nGramCount.readFields(dataInput);
		mostFreqNGram.readFields(dataInput);
		decade.readFields(dataInput);
	}

	public Text getnGram() {
		return nGram;
	}

	public void setnGram(Text nGram) {
		this.nGram = nGram;
	}

	public int getnGramCount() {
		return nGramCount.get();
	}

	public void setnGramCount(IntWritable nGramCount) {
		this.nGramCount = nGramCount;
	}

	public IntWritable getMostFreqNGram() {
		return mostFreqNGram;
	}

	public void setMostFreqNGram(IntWritable mostFreqNGram) {
		this.mostFreqNGram = mostFreqNGram;
	}

	public Text getDecade() {
		return decade;
	}

	public void setDecade(Text decade) {
		this.decade = decade;
	}

	@Override
	public int hashCode() {
		return 0;
		// This is used by HashPartitioner, so implement it if you need it.
	}

	@Override
	public String toString() {
		return " " + nGramCount + " " + mostFreqNGram + " " + decade;
	}
} // end class

