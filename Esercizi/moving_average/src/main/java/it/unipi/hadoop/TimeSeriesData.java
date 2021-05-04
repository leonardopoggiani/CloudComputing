package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeSeriesData implements Writable,Comparable<TimeSeriesData>{

    private double value;
    private long timestamp;

    public TimeSeriesData() {
    }

    public TimeSeriesData(double val, long time) {
        setValue(val);
        setTimestamp(time);
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(it.unipi.hadoop.TimeSeriesData o) {
        long thisTimestamp = this.timestamp;
        long thatTimestamp = o.getTimestamp();
        return Long.compare(thisTimestamp, thatTimestamp);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(value);
        dataOutput.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value = dataInput.readDouble();
        timestamp = dataInput.readLong();
    }

    public String toString() {
        return timestamp + ", " + value;
    }
}
