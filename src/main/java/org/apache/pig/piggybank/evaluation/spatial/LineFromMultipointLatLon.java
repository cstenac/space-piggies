package org.apache.pig.piggybank.evaluation.spatial;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTWriter;


public class LineFromMultipointLatLon extends EvalFunc<String> {
    protected GeometryFactory f = new GeometryFactory(new PrecisionModel(), 4326);
    protected WKTWriter writer = new WKTWriter();

    public LineFromMultipointLatLon() {
        super();
    }

    private String strOrByteArrayToStr(Tuple tuple, int column) throws ExecException, UnsupportedEncodingException {
        String ret = null;
        // We don't control what's in the bag through the schema so we have to test
        Object o = tuple.get(column);
        if (o instanceof DataByteArray) {
            ret = new String(((DataByteArray)o).get(), "utf8");
        } else {
            ret= (String)o;
        }
        return ret;
    }

    /**
     * Creates a LINESTRING WKT column from two bags containing latitudes and longitudes
     * @param input tuple; first column contains the bag of latitudes, second column contains the bag of longitudes
     * @exception IOException
     */
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null)
            return null;

        try {
            DataBag latBag = (DataBag)input.get(0);
            DataBag lonBag = (DataBag)input.get(1);

            if (latBag.size() != lonBag.size()) {
                warn("Inconsistent bags " + latBag.size() +  " lats, " + lonBag.size() + " longs",
                        PigWarning.UDF_WARNING_1);
            }

            List<Coordinate> pointCoordinates = new ArrayList<Coordinate>();
            Iterator latIterator = latBag.iterator();
            Iterator lonIterator = lonBag.iterator();
            for (int i = 0; i < latBag.size(); i++) {
                String lat = strOrByteArrayToStr((Tuple)latIterator.next(), 0);
                String lon = strOrByteArrayToStr((Tuple)lonIterator.next(), 0);
                pointCoordinates.add(new Coordinate(Double.parseDouble(lon), Double.parseDouble(lat)));
            }

            LineString ls = f.createLineString(pointCoordinates.toArray(new Coordinate[0]));
            return writer.write(ls);
        }
        catch (ClassCastException e) {
            warn("unable to cast input "+input.get(0)+" of class "+
                    input.get(0).getClass()+" to String", PigWarning.UDF_WARNING_1);
            return null;
        } 
        catch(Exception e){
            e.printStackTrace();
            warn("Error processing input "+input.get(0) + ": " + e, PigWarning.UDF_WARNING_1);
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
        Schema inSchema = new Schema();
        inSchema.add(new Schema.FieldSchema(null, DataType.BAG));
        inSchema.add(new Schema.FieldSchema(null, DataType.BAG));
        funcList.add(new FuncSpec(this.getClass().getName(), inSchema));
        return funcList;
    }
}