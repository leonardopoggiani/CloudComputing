package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixData implements Writable {

    private int row;
    private int column;
    private double value;

    public MatrixData() {
    }

    public MatrixData(int row, int column, double value) {
        setRow(row);
        setColumn(column);

    }

    private void setColumn(int column) {
    }

    private void setRow(int row) {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
