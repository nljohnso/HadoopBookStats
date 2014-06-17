package cs455.hadoop.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class implements Writable and contains ngram count and decade span.
 * 
 * @author nljohnso
 * 
 */
public class NGramWritable implements Writable {

	private IntWritable nGramCount;
	private Text decade;

	public NGramWritable() {
		this.nGramCount = new IntWritable();
		this.decade = new Text();
	}

	public NGramWritable(IntWritable nGramCount, Text decade) {
		this.nGramCount = nGramCount;
		this.decade = decade;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		nGramCount.readFields(dataInput);
		decade.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		nGramCount.write(dataOutput);
		decade.write(dataOutput);
	}

	public IntWritable getnGramCount() {
		return nGramCount;
	}

	public void setnGramCount(IntWritable nGramCount) {
		this.nGramCount = nGramCount;
	}

	public Text getDecade() {
		return decade;
	}

	public void setDecade(Text decade) {
		this.decade = decade;
	}

	public String toString() {
		return nGramCount + " " + decade;
	}

}
