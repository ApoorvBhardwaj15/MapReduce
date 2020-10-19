package s3757978.mapReduce.Assignment_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

// custom class TextPair
public class TextPair implements Writable,WritableComparable<TextPair> {
	
	private Text first, second;  //c
	
	// constructors
	public TextPair() {
		this.first = new Text();
		this.second = new Text();
	}
	
	public TextPair(Text first, Text second) {
		set(first,second);
	}
	
	// setter methods
	private void set(Text first2, Text second2) {
		// TODO Auto-generated method stub
		this.first = first2;
		this.second = second2;
	}
	
	// constructor
	public TextPair(String first, String second) {
		this.first = new Text(first);
		this.second = new Text(second);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
	this.first.readFields(arg0);
	this.second.readFields(arg0);
	
	}
	
	public static TextPair read(DataInput in) throws IOException {
		TextPair wordPair = new TextPair();
	    wordPair.readFields(in);
	    return wordPair;
	    }
	

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		first.write(arg0);
		second.write(arg0);
		
	}

	@Override
	// changed to ensure ordering
	public int compareTo(TextPair o) {
		// TODO Auto-generated method stub
		 int returnVal = this.first.compareTo(o.getFirst());
	        if(returnVal != 0){
	            return returnVal;
	        }
	        if(this.second.toString().equals("*")){
	            return -1;
	        }else if(o.getSecond().toString().equals("*")){
	            return 1;
	        }
	        return this.second.compareTo(o.getSecond());

	}
	
	// overridden method, it defines how the words will be written in hdfs
	 public String toString() {
		 String a = first.toString();
		 String b = second.toString();
		 return "( "+a +" , "+b+" )";
	              
	    }
	
	 // method to check equality of a textpair
	 public boolean equals(Object o) {
	        if (this == o) return true;
	        if (o == null || getClass() != o.getClass()) return false;

	        TextPair wordPair = (TextPair) o;

	        if (second != null ? !second.equals(wordPair.second) : wordPair.second != null) return false;
	        if (first != null ? !first.equals(wordPair.first) : wordPair.first != null) return false;

	        return true;
	    }
	 

	 // getter and setters
	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}


}
