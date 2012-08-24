package org.apache.pig.piggybank.test.evaluation.spatial;

import static org.junit.Assert.*;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.spatial.LineFromMultipoint;
import org.apache.pig.piggybank.evaluation.spatial.LineFromMultipointLatLon;
import org.junit.Test;

public class TestLineFromMultipointLatLong {
    private Tuple newTuple(String... latsAndLongs) throws ExecException {
        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        DataBag latBag = BagFactory.getInstance().newDefaultBag();
        DataBag lonBag = BagFactory.getInstance().newDefaultBag();
        
        for (int i = 0; i < latsAndLongs.length; i+= 2) {
            latBag.add(TupleFactory.getInstance().newTuple(latsAndLongs[i]));
            lonBag.add(TupleFactory.getInstance().newTuple(latsAndLongs[i+1]));
        }
        t1.set(0, latBag);
        t1.set(1, lonBag);
        System.out.println("SENDING " + t1);
        return t1;
    }
    
    @Test
    public void emptyBag() throws Exception {
        Tuple t1 = newTuple();
        
        LineFromMultipointLatLon func = new LineFromMultipointLatLon();
        assertEquals("LINESTRING EMPTY", func.exec(t1));
    }
    @Test
    public void regular1() throws Exception {
        Tuple t1 = newTuple("0", "0", "1", "1");
        LineFromMultipointLatLon func = new LineFromMultipointLatLon();
        assertEquals("LINESTRING (0 0, 1 1)", func.exec(t1));
    }
    @Test
    public void closed() throws Exception {
        Tuple t1 = newTuple("0", "0", "1", "1", "0", "0");
        LineFromMultipointLatLon func = new LineFromMultipointLatLon();
        assertEquals("LINESTRING (0 0, 1 1, 0 0)", func.exec(t1));
    }
}
