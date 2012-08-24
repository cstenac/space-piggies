package org.apache.pig.piggybank.evaluation.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.operation.polygonize.Polygonizer;


public class Polygonize extends EvalFunc<DataBag> {
    GeometryFactory f = new GeometryFactory();
    BagFactory bf = BagFactory.getInstance();
    TupleFactory tf = TupleFactory.getInstance();
    WKTReader reader = new WKTReader();
    WKTWriter writer = new WKTWriter();

    public Polygonize() {
        super();
    }

    /**
     * Creates a bag containing the list of possible polygons WKT from a bag containing a list of linestring WKT 
     * @param input tuple; first column contains the bag, first column of each tuple in the bag is the linestring WKT
     * @exception IOException
     */
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null)
            return null;
        
        DataBag out = bf.newDefaultBag();
        
        Polygonizer polygonizer = new Polygonizer();
        try {
            DataBag db = (DataBag)input.get(0);
            for (Tuple t : db) {
                // We don't control what's in the bag through the schema so we have to test
                Object column = t.get(0);
                String wkt = null;
                if (column instanceof DataByteArray) {
                    wkt = new String(((DataByteArray)column).get(), "utf8");
                } else {
                    wkt = (String)t.get(0);
                }
                Geometry line = reader.read(wkt);
                polygonizer.add(line);
            }
           
            for (Object ogeom : polygonizer.getPolygons()) {
                Geometry geom = (Geometry)ogeom;
                out.add(tf.newTuple(writer.write(geom)));
            }
            log.info("Found " + polygonizer.getPolygons().size()  +" polygons");
            return out;
        }
        catch (ClassCastException e) {
            warn("unable to cast input "+input.get(0)+" of class "+
                    input.get(0).getClass()+" to String", PigWarning.UDF_WARNING_1);
            return null;
        } 
        catch (ParseException e) {
            warn("failed to read Point WKT from column", PigWarning.UDF_WARNING_1);
            return null;
        }
        catch(Exception e){
            warn("Error processing input "+input.get(0), PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    /**
     * This method gives a name to the column. 
     * @param input - schema of the input data
     * @return schema of the input data
     */
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName("polygons", input), DataType.BAG));
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.BAG))));
        return funcList;
    }
}