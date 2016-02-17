package org.neo4j.tpcH.resources;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import com.codahale.metrics.annotation.Timed;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.wordnik.swagger.annotations.*;

@Path("/TpcH/Normalized")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/TpcH/Normalized", description = "testing tpcH queries with normalized data model")
public class TPCHResource {

	private static final String CLASS_NAME = TPCHResource.class.getName();
	private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

	GraphDatabaseService graphDb;

	public TPCHResource(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
	}

	@GET
	@Path("/q1/")
	@Timed
	@ApiOperation(value = "get result of TPCH Q1 using this model", notes = "Returns JsonObject", response = JsonObject.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "internal server error !") })
	public JsonObject getQ1Results() {

		JsonObject result = null;
		;

		try {

			return result;
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
	@Path("/q3/")
	@Timed
	@ApiOperation(value = "get result of TPCH Q3 using this model", notes = "Returns Result", response = Result.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "internal server error !") })
	public Result getQ2Results() {

		Result result = null;

		try {

			try (Transaction ignored = this.graphDb.beginTx()) {
				result = this.graphDb
						.execute("match (n {name: 'my node'}) return n, n.name");
			}

			return result;
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
	@Path("/q4/")
	@Timed
	@ApiOperation(value = "get result of TPCH Q4 using this model", notes = "Returns JsonObject", response = JsonObject.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "internal server error !") })
	public JsonObject getQ3Results() {

		JsonObject result = null;

		try {
			return result;
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
