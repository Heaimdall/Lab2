package Flights_lab3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by heimdall on 22.09.17.
 */
public class FlightsWritableComparable implements WritableComparable {
    private int flag;
    private int destAeroportId;

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public void setDestAeroportId(int destAirportId) {
        this.destAeroportId = destAirportId;
    }

    public int getDestAeroportId() {
        return destAeroportId;
    }



    @Override
    public int compareTo(Object o) {

        FlightsWritableComparable obj = (FlightsWritableComparable) o;

        if (this.destAeroportId > obj.destAeroportId) {
            return 1;
        }
        else if (this.destAeroportId < obj.destAeroportId) {
            return -1;
        }
        else if (this.flag > obj.flag) {
            return 1;
        }
        else if (this.flag < obj.flag) {
            return -1;
        }

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(flag);
        dataOutput.writeInt(destAeroportId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flag = dataInput.readInt();
        destAeroportId = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public String toString() {
        return "String : " + flag + " , " + destAeroportId;
    }
}
