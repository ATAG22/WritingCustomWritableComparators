package com.atag.poc.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PersonWritable implements WritableComparable<PersonWritable> {

	private Text FirstName;
	private Text SecondName;
	private IntWritable age;
	
	
	
	
	public Text getFirstName() {
		return FirstName;
	}

	public void setFirstName(Text firstName) {
		FirstName = firstName;
	}

	public Text getSecondName() {
		return SecondName;
	}

	public void setSecondName(Text secondName) {
		SecondName = secondName;
	}

	public IntWritable getAge() {
		return age;
	}

	public void setAge(IntWritable age) {
		this.age = age;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.FirstName = new Text(arg0.readLine());
		this.SecondName = new Text(arg0.readLine());
		this.age = new IntWritable();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		FirstName.write(arg0);
		SecondName.write(arg0);
		age.write(arg0);
	}

	@Override
	public int compareTo(PersonWritable o) {
		// TODO Auto-generated method stub
		Text FirstName = o.getFirstName();
		Text SecondName = o.getSecondName();
		
		int lengthOfFirstName = FirstName.getLength();
		int lengthOfSecondName = SecondName.getLength();
		// comparing first names
		int valueOfFirstComparison = this.FirstName.compareTo(FirstName);
		
		if(valueOfFirstComparison != 0)
		{
			return valueOfFirstComparison;
		}
		//comparing second names
		int valueOfSecondComparison = this.SecondName.compareTo(SecondName);
		
		if(valueOfSecondComparison != 0)
		{
			return valueOfSecondComparison;
		}
		//comparing ages
		int valueOfAgeComparison = this.age.compareTo(age);
		{
			return valueOfAgeComparison;
		}
	}
	
	

}
