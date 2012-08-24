package org.apache.pig.piggybank.evaluation.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;


public class MultiIntersectionComputer extends EvalFunc<DataBag> {
    GeometryFactory f = new GeometryFactory();
    WKTReader reader = new WKTReader();
    WKTWriter writer = new WKTWriter();
    private boolean setup;
    private Quadtree index = new Quadtree();
    private static BagFactory bf = BagFactory.getInstance();
    private static TupleFactory tf = TupleFactory.getInstance();

    public MultiIntersectionComputer() {
        super();
    }

    /**
     * Takes as first argument a String with a DFS filename containing a list of (geometry_id, geometry_wkt).
     * (The RIGHT geometries)
     * 
     * Takes as second argument, in each tuple of the relation, the wkt of a geometry (the LEFT geometry)
     * 
     * Outputs a databag containing tuples (matched_geometry_id , intersection_wkt), for each of the 
     * items in the RIGHT geometries intersected by the LEFT geometry.

     * @param input tuple; first column contains the DFS file path, second column the LEFT geometry wkt (string)
     * for each tuple 
     * @exception IOException
     */
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null)
            return null;

        try {
            if (!setup) {
                String filePath = (String)input.get(0); 
                setUp(filePath);
                setup = true;
            }
        } catch (ParseException e) {
            throw new IOException("Could not read right geometries", e);
        }

        try {
            DataBag out = bf.newDefaultBag(); 
            String geomWKT = (String)input.get(1);
            Geometry leftGeom = reader.read(geomWKT);

            @SuppressWarnings("unchecked")
            List<Tuple> tuples = index.query(leftGeom.getEnvelopeInternal());
            if (log.isDebugEnabled()) {
                log.debug("** Potential matches for " + leftGeom +"  -> " + tuples.size());
            }

            for (Tuple potentialMatch : tuples) {
                Geometry intersection = leftGeom.intersection((Geometry)potentialMatch.get(2));
                if (!intersection.isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("true match found");
                    }
                    Tuple match = tf.newTuple();
                    match.append(new String(((DataByteArray)potentialMatch.get(0)).get(), "utf8"));
                    match.append(writer.write(intersection));
                    out.add(match);
                }
            }
            return out;
        }
        catch (ClassCastException e) {
            warn("unable to cast input "+input.get(0)+" of class "+
                    input.get(0).getClass()+" to String", PigWarning.UDF_WARNING_1);
            return null;
        } 
        catch (ParseException e) {
            warn("failed to read Point WKT from column: " + e, PigWarning.UDF_WARNING_1);
            return null;
        }
        catch(Exception e){
            warn("Error processing input "+input.get(0), PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    static final protected Tuple dummyTuple = null;

    private void setUp(String fileName) throws IOException, ParseException {
        POLoad loader = new POLoad(new OperatorKey("MultiIntersectionComputerLoader",
                NodeIdGenerator.getGenerator().getNextNodeId("MultiIntersectionComputerLoader")),
                new FileSpec(fileName, new FuncSpec("PigStorage", "\t")));

        Properties props = ConfigurationUtil.getLocalFSProperties();
        PigContext pc = new PigContext(ExecType.LOCAL, props);   
        loader.setPc(pc);
        loader.setUp();

        long loadedRG = 0;
        for (Result res = loader.getNext(dummyTuple);res.returnStatus != POStatus.STATUS_EOP;res = loader.getNext(dummyTuple)) {
            ++loadedRG;
            if (reporter != null)
                reporter.progress();               
            Tuple tuple = (Tuple) res.result;
            String wkt = null;
            if (tuple.get(1) instanceof DataByteArray) {
                wkt = new String(((DataByteArray)tuple.get(1)).get(), "utf8");
            } else {
                wkt = (String)tuple.get(1);
            }
            Geometry geom = reader.read(wkt);
            tuple.append(geom);

            log.info("** Loading into index " + geom);
            index.insert(geom.getEnvelopeInternal(), tuple);
        }
        log.info("Loaded " + loadedRG  +" right geometry objects");
    }

    /**
     * This method gives a name to the column. 
     * @param input - schema of the input data
     * @return schema of the input data
     */
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName("matches", input), DataType.BAG));
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema inSchema = new Schema();
        inSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        inSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), inSchema));
        return funcList;
    }
}