package org.apache.pig.piggybank.test.evaluation.spatial;

import static org.junit.Assert.*;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.spatial.LineFromMultipoint;
import org.junit.Test;

public class TestLineFromMultipoint {
    private Tuple newTuple(String... points) throws ExecException {
        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        
        for (String point : points) {
            bag.add(TupleFactory.getInstance().newTuple(point));
        }
        t1.set(0, bag);
        return t1;
    }
    
    @Test
    public void emptyBag() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, BagFactory.getInstance().newDefaultBag());
        
        LineFromMultipoint func = new LineFromMultipoint();
        assertEquals("LINESTRING EMPTY", func.exec(t1));
    }
    @Test
    public void regular1() throws Exception {
        Tuple t1 = newTuple("POINT(0 0)", "POINT(1 1)");
        LineFromMultipoint func = new LineFromMultipoint();
        assertEquals("LINESTRING (0 0, 1 1)", func.exec(t1));
    }
    @Test
    public void closed() throws Exception {
        Tuple t1 = newTuple("POINT(0 0)", "POINT(1 1)", "POINT(0 0)");
        LineFromMultipoint func = new LineFromMultipoint();
        assertEquals("LINESTRING (0 0, 1 1, 0 0)", func.exec(t1));
    }
}
