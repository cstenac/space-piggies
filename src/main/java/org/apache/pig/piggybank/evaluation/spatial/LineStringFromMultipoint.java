package org.apache.pig.piggybank.evaluation.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;


public class LineStringFromMultipoint extends EvalFunc<String> {
    GeometryFactory f = new GeometryFactory();
    WKTReader reader = new WKTReader();
    WKTWriter writer = new WKTWriter();

    public LineStringFromMultipoint() {
        super();
    }

    /**
     * Creates a LINESTRING WKT column from a bag of POINTs. 
     * @param input tuple; first column contains the bag, first column of each tuple in the bag is the point WKT
     * @exception IOException
     */
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null)
            return null;
        
        try {
            DataBag db = (DataBag)input.get(0);
            List<Geometry> points = new ArrayList<Geometry>();
            for (Tuple t : db) {
                // We don't control what's in the bag through the schema so we have to test
                Object column = t.get(0);
                String wkt = null;
                if (column instanceof DataByteArray) {
                    wkt = new String(((DataByteArray)column).get(), "utf8");
                } else {
                    wkt = (String)t.get(0);
                }
                Geometry p = reader.read(wkt);
                points.add(p);
            }
            List<Coordinate> pointCoordinates = new ArrayList<Coordinate>();
            for (Geometry p : points) {
                pointCoordinates.add(p.getCoordinate());
            }

            LineString ls = f.createLineString(pointCoordinates.toArray(new Coordinate[0]));
            return writer.write(ls);
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
        return new Schema(new Schema.FieldSchema(getSchemaName("linestring", input), DataType.CHARARRAY));
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