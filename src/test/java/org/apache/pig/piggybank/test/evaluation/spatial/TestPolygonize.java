package org.apache.pig.piggybank.test.evaluation.spatial;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.spatial.LineFromMultipoint;
import org.apache.pig.piggybank.evaluation.spatial.Polygonize;
import org.junit.Test;

public class TestPolygonize {
    private Tuple newTuple(String... linestrings) throws ExecException {
        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        DataBag bag = BagFactory.getInstance().newDefaultBag();

        for (String point : linestrings) {
            bag.add(TupleFactory.getInstance().newTuple(point));
        }
        t1.set(0, bag);
        return t1;
    }

    @Test
    public void emptyBag() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, BagFactory.getInstance().newDefaultBag());

        Polygonize func = new Polygonize();
        DataBag polygons = func.exec(t1);
        assertEquals(0, polygons.size());
    }
    @Test
    public void oneClosedWay() throws Exception {
        Tuple t1 = newTuple("LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)");
        Polygonize func = new Polygonize();
        DataBag polygons = func.exec(t1);
        assertEquals(1, polygons.size());
        assertEquals("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", ((Tuple)polygons.iterator().next()).get(0));
    }
    @Test
    public void twoWaysOnePolygon() throws Exception {
        Tuple t1 = newTuple("LINESTRING(0 0, 1 0, 1 1)", "LINESTRING(1 1, 0 1, 0 0)");
        Polygonize func = new Polygonize();
        DataBag polygons = func.exec(t1);
        assertEquals(1, polygons.size());
        assertTrue("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))".equals(((Tuple)polygons.iterator().next()).get(0))
        || "POLYGON ((1 1, 1 0, 0 0, 0 1, 1 1))".equals(((Tuple)polygons.iterator().next()).get(0)));
        
        ;
    }
    @Test
    public void severalPolygons() throws Exception {
        Tuple t1 = newTuple("LINESTRING(0 0, 1 0, 1 1)", "LINESTRING(1 1, 0 1, 0 0)", "LINESTRING(1 1, 2 -4, 0 0)");
        Polygonize func = new Polygonize();
        DataBag polygons = func.exec(t1);
        assertEquals(2, polygons.size());
        assertTrue("POLYGON ((1 1, 2 -4, 0 0, 1 0, 1 1))".equals(((Tuple)polygons.iterator().next()).get(0)) ||
                "POLYGON ((0 0, 1 0, 1 1, 2 -4, 0 0))".equals(((Tuple)polygons.iterator().next()).get(0))); 

        Iterator i = polygons.iterator();
        i.next();
        Tuple t = (Tuple)i.next();
        assertTrue("POLYGON ((1 1, 1 0, 0 0, 0 1, 1 1))".equals(t.get(0)) ||
                "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))".equals(t.get(0)));

    }

    /*
    @Test
    public void closed() throws Exception {
        Tuple t1 = newTuple("POINT(0 0)", "POINT(1 1)", "POINT(0 0)");
        LineFromMultipoint func = new LineFromMultipoint();
        assertEquals("LINESTRING (0 0, 1 1, 0 0)", func.exec(t1));
    }
     */
}
