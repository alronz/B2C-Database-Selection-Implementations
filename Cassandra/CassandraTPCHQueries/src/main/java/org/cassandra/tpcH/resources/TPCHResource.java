package org.cassandra.tpcH.resources;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.datastax.driver.core.*;
import com.codahale.metrics.annotation.Timed;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.wordnik.swagger.annotations.*;
import org.cassandra.tpcH.TPCHModel;

@Path("/TpcH")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/TpcH", description = "testing tpcH queries with normalized data model")
public class TPCHResource {

    private static final String CLASS_NAME = TPCHResource.class.getName();
    private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

    TPCHModel model;


    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    public TPCHResource(TPCHModel model) {
        this.model = model;
    }


    @GET
    @Path("/q1")
    @Timed
    @ApiOperation(value = "get result of TPCH Q1 using this model", notes = "Returns String", response = String.class)
    @ApiResponses(value = {@ApiResponse(code = 500, message = "internal server error !")})
    public String getQ1Results() {


        try {

            String q1Statement = "SELECT returnflag, linestatus, sum(quantity) as sum_qty,sum(extendedprice) as sum_base_price, sum(CASSANDRA_EXAMPLE_KEYSPACE.fSumDiscPrice(extendedprice,discount)) as sum_disc_price\n" +
                    ", sum(CASSANDRA_EXAMPLE_KEYSPACE.fSumChargePrice(extendedprice,discount,tax)) as sum_charge, avg(quantity) as avg_qty,\n" +
                    "avg(extendedprice) as avg_price, avg(discount) as avg_disc, count(*) as count_order, orderkey ,linenumber \n" +
                    "FROM CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q1 WHERE shipdate < '2000-01-01 22:00:00-0700' and orderkey='5' and linenumber = '5' and returnflag='N' and linestatus = 'O' ; ";


            LOGGER.info(q1Statement);


            this.model.connect();
            ResultSet rsQ1 = this.model.executeStatement(q1Statement);
            this.model.close();

            return rsQ1.all().toString();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,
                    "internal server error !" + e.getLocalizedMessage());
            final String shortReason = "internal server error !";
            Exception cause = new IllegalArgumentException(shortReason);
            throw new WebApplicationException(cause,
                    javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("/q3")
    @Timed
    @ApiOperation(value = "get result of TPCH Q3 using this model", notes = "Returns String", response = String.class)
    @ApiResponses(value = {@ApiResponse(code = 500, message = "internal server error !")})
    public String getQ2Results() {


        try {

            String tpchQ3TempTable = " CREATE TABLE IF NOT EXISTS CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q3_TEMP\n" +
                    " (\n" +
                    " orderkey text,\n" +
                    " linenumber text,\n" +
                    " revenue double,\n" +
                    " o_orderdate timestamp,\n" +
                    " o_shippriority text,\n" +
                    " PRIMARY KEY ((orderkey,linenumber),revenue,o_orderdate) \n" +
                    " )WITH CLUSTERING ORDER BY (revenue DESC, o_orderdate ASC);";


            this.model.connect();
            this.model.executeStatement(tpchQ3TempTable);


            String q3TempStatement = "SELECT orderkey,sum(CASSANDRA_EXAMPLE_KEYSPACE.fSumDiscPrice(l_extendedprice,l_discount)) as revenue, o_orderdate,l_shipdate, o_shippriority,linenumber from CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q3\n" +
                    " where orderkey= '1' and linenumber='1' and o_orderdate = '1996-01-01 23:00:00+0000' and o_shippriority='5-LOW' and  c_mktsegment= 'AUTOMOBILE' and l_shipdate > '1990-01-01' ;";


            ResultSet rsQ3Temp = this.model.executeStatement(q3TempStatement);
            LOGGER.info(rsQ3Temp.toString());

            // insert the results into the temp table
            for (Row row : rsQ3Temp.all()) {
                LOGGER.info(row.toString());
                LOGGER.info(formatter.format(row.getTimestamp("o_orderdate")).toString());
                String insertStatement = "INSERT INTO CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q3_TEMP (orderkey,revenue,o_orderdate," +
                        "o_shippriority,linenumber) VALUES" +
                        " ('" + row.getString("orderkey") +
                        "'," + Double.valueOf(row.getDouble("revenue")) +
                        ",'" + formatter.format(row.getTimestamp("o_orderdate")) +
                        "','" + row.getString("o_shippriority") +
                        "','" + row.getString("linenumber") + "') IF NOT EXISTS;";

                ResultSet rsQ3Insert = this.model.executeStatement(insertStatement);
                LOGGER.info(rsQ3Insert.toString());
            }


            String q3FinalStatement = "SELECT orderkey, revenue, o_orderdate, o_shippriority  from CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q3_TEMP where orderkey='1' and linenumber = '1'; ";


            ResultSet rsQ3 = this.model.executeStatement(q3FinalStatement);

            String dropQ3TempTable = "DROP TABLE CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q3_TEMP;";

            ResultSet rsQ3DropMV = this.model.executeStatement(dropQ3TempTable);
            LOGGER.info(rsQ3DropMV.toString());

            this.model.close();

            return rsQ3.all().toString();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,
                    "internal server error !" + e.getLocalizedMessage());
            final String shortReason = "internal server error !";
            Exception cause = new IllegalArgumentException(shortReason);
            throw new WebApplicationException(cause,
                    javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("/q4")
    @Timed
    @ApiOperation(value = "get result of TPCH Q4 using this model", notes = "Returns String", response = String.class)
    @ApiResponses(value = {@ApiResponse(code = 500, message = "internal server error !")})
    public String getQ3Results() {

        try {

            String createIndexOnCommitDate = "CREATE INDEX IF NOT EXISTS l_commitdate_index ON CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q4 (l_commitdate)";

            String q4Statement = "SELECT o_orderpriority, count(*) as order_count from CASSANDRA_EXAMPLE_KEYSPACE.TPCH_Q4 \n" +
                    "where orderkey= '1' and linenumber='1' and o_orderpriority ='5-LOW' and o_orderdate >=  '1990-01-01' and o_orderdate < '2000-01-01' and l_commitdate < '2000-01-01' ALLOW FILTERING ";


            this.model.connect();
            this.model.executeStatement(createIndexOnCommitDate);
            ResultSet rsQ4 = this.model.executeStatement(q4Statement);
            this.model.close();

            return rsQ4.all().toString();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,
                    "internal server error !" + e.getLocalizedMessage());
            final String shortReason = "internal server error !";
            Exception cause = new IllegalArgumentException(shortReason);
            throw new WebApplicationException(cause,
                    javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

}
