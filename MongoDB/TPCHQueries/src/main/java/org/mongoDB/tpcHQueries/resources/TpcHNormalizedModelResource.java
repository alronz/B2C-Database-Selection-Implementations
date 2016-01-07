package org.mongoDB.tpcHQueries.resources;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.codahale.metrics.annotation.Timed;
import com.google.gson.Gson;
import com.wordnik.swagger.annotations.*;

@Path("/TpcH/Normalized")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/TpcH/Normalized", description = "testing tpcH queries with normalized data model")
public class TpcHNormalizedModelResource {

	private static final String CLASS_NAME = TpcHNormalizedModelResource.class
			.getName();
	private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

	private MongoClient mongoClient;
	private MongoDatabase database;

	private MongoCollection<Document> normalized_customer;
	private MongoCollection<Document> normalized_supplier;
	private MongoCollection<Document> normalized_partsupp;
	private MongoCollection<Document> normalized_order;
	private MongoCollection<Document> normalized_region;
	private MongoCollection<Document> normalized_nation;
	private MongoCollection<Document> normalized_part;
	private MongoCollection<Document> normalized_lineitem;

	public TpcHNormalizedModelResource(MongoClient mongoClient) {

		this.mongoClient = mongoClient;

		this.database = this.mongoClient.getDatabase("mydb");

		normalized_customer = this.database
				.getCollection("normalized_customer");
		normalized_supplier = this.database
				.getCollection("normalized_supplier");
		normalized_partsupp = this.database
				.getCollection("normalized_partsupp");
		normalized_order = this.database.getCollection("normalized_order");
		normalized_region = this.database.getCollection("normalized_region");
		normalized_nation = this.database.getCollection("normalized_nation");
		normalized_part = this.database.getCollection("normalized_part");
		normalized_lineitem = this.database
				.getCollection("normalized_lineitem");

	}

	@GET
	@Path("/q1/")
	@Timed
	@ApiOperation(value = "get result of TPCH Q1 using this model", notes = "Returns mongoDB document(s)", response = Document.class, responseContainer = "list")
	@ApiResponses(value = { @ApiResponse(code = 500, message = "internal server error !") })
	public Document getQ1Results() {

		AggregateIterable<Document> result;

		try {

			String matchStringQuery = "{\"$match\":{\"shipdate\":{\"$lte\":ISODate(\"2016-01-01T00:00:00.000Z\")}}}";

			String projectStringQuery = "{\"$project\":{\"returnflag\":1,\"linestatus\":1,\"quantity\":1,\"extendedprice\":1,\"discount\":1,\"l_dis_min_1\":{\"$subtract\":[1,\"$discount\"]},\"l_tax_plus_1\":{\"$add\":[\"$tax\",1]}}}";

			String groupStringQuery = "{\"$group\":{\"_id\":{\"l_returnflag\":\"$returnflag\",\"l_linestatus\":\"$linestatus\"},\"sum_qty\":{\"$sum\":\"$quantity\"},\"sum_base_price\":{\"$sum\":\"$extendedprice\"},\"sum_disc_price\":{\"$sum\":{\"$multiply\":[\"$extendedprice\",\"$l_dis_min_1\"]}},\"sum_charge\":{\"$sum\":{\"$multiply\":[\"$extendedprice\",{\"$multiply\":[\"$l_tax_plus_1\",\"$l_dis_min_1\"]}]}},\"avg_price\":{\"$avg\":\"$extendedprice\"},\"avg_disc\":{\"$avg\":\"$discount\"},\"count_order\":{\"$sum\":1}}}";

			String sortStringQuery = "{\"$sort\":{\"returnflag\":1,\"linestatus\":1}}";

			// String out = "{\"$out\":\"out\"}";

			BsonDocument matchBsonQuery = BsonDocument.parse(matchStringQuery);

			BsonDocument projectBsonQuery = BsonDocument
					.parse(projectStringQuery);

			BsonDocument groupBsonQuery = BsonDocument.parse(groupStringQuery);

			BsonDocument sortBsonQuery = BsonDocument.parse(sortStringQuery);

			// BsonDocument outBson = BsonDocument.parse(out);

			LOGGER.info("matchBsonQuery is " + matchBsonQuery.toJson());

			LOGGER.info("groupBsonQuery is " + groupBsonQuery.toJson());

			LOGGER.info("sortBsonQuery is " + sortBsonQuery.toJson());

			ArrayList<Bson> aggregateQuery = new ArrayList<Bson>();

			aggregateQuery.add(matchBsonQuery);
			// aggregateQuery.add(outBson);
			aggregateQuery.add(projectBsonQuery);
			aggregateQuery.add(groupBsonQuery);
			aggregateQuery.add(sortBsonQuery);

			result = this.normalized_lineitem.aggregate(aggregateQuery);

			// LOGGER.info("result is " +result.first().toJson());

			return result.first();
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
	@ApiOperation(value = "get result of TPCH Q3 using this model", notes = "Returns mongoDB document(s)", response = Document.class, responseContainer = "list")
	@ApiResponses(value = { @ApiResponse(code = 500, message = "internal server error !") })
	public Document getQ2Results() {

		AggregateIterable<Document> result;

		try {

			BsonDocument bsonQuery = BsonDocument
					.parse("{\"orderdate\":{\"$lte\":ISODate(\"2016-01-01T00:00:00.000Z\") }}");

			this.database.createCollection("normalized_q3_new_joined_orders");

			final MongoCollection<Document> normalized_q3_new_joined_orders = this.database
					.getCollection("normalized_q3_new_joined_orders");

			this.normalized_order.find(bsonQuery).forEach(
					new Block<Document>() {
						@Override
						public void apply(final Document order) {
							BsonDocument customerBsonQuery = BsonDocument
									.parse("{\"_id\":\""
											+ order.get("customer") + "\"}");

							order.put("customer",
									normalized_customer.find(customerBsonQuery)
											.first());

							BsonDocument lineitemsBsonQuery = BsonDocument
									.parse("{\"_id\":{\"$in\":"
											+ new Gson().toJson(order
													.get("lineitems")) + "}}");

							order.put("lineitems", normalized_lineitem
									.find(lineitemsBsonQuery));

							normalized_q3_new_joined_orders.insertOne(order);
						}
					});

			String matchStringQuery = "{\"$match\":{\"customer.mktsegment\":\"some text\",\"lineitems.shipdate\":{\"$gte\": ISODate(\"2015-01-01T00:00:00.000Z\") }}}";

			String unWindStringQuery = "{$unwind: \"$lineitems\"}";

			String projectStringQuery = "{\"$project\":{\"orderdate\":1,\"shippriority\":1,\"lineitems.extendedprice\":1,\"l_dis_min_1\":{\"$subtract\":[1,\"$lineitems.discount\"]}}}";

			String groupStringQuery = "{\"$group\":{\"_id\":{\"l_orderkey\":\"$_id\",\"o_orderdate\":\"$orderdate\",\"o_shippriority\":\"$shippriority\"},\"revenue\":{\"$sum\":{\"$multiply\":[\"$lineitems.extendedprice\",\"$l_dis_min_1\"]}}}}";

			String sortStringQuery = "{\"$sort\":{\"revenue\":1,\"o_orderdate\":1}}";

			// String out = "{\"$out\":\"out\"}";

			BsonDocument matchBsonQuery = BsonDocument.parse(matchStringQuery);

			BsonDocument unWindBsonQuery = BsonDocument
					.parse(unWindStringQuery);

			BsonDocument projectBsonQuery = BsonDocument
					.parse(projectStringQuery);

			BsonDocument groupBsonQuery = BsonDocument.parse(groupStringQuery);

			BsonDocument sortBsonQuery = BsonDocument.parse(sortStringQuery);

			// BsonDocument outBson = BsonDocument.parse(out);

			LOGGER.info("matchBsonQuery is " + matchBsonQuery.toJson());

			LOGGER.info("groupBsonQuery is " + groupBsonQuery.toJson());

			LOGGER.info("sortBsonQuery is " + sortBsonQuery.toJson());

			ArrayList<Bson> aggregateQuery = new ArrayList<Bson>();

			aggregateQuery.add(matchBsonQuery);
			// aggregateQuery.add(outBson);
			aggregateQuery.add(unWindBsonQuery);
			aggregateQuery.add(projectBsonQuery);
			aggregateQuery.add(groupBsonQuery);
			aggregateQuery.add(sortBsonQuery);

			result = normalized_q3_new_joined_orders.aggregate(aggregateQuery);

			// LOGGER.info("result is " +result.first().toJson());

			return result.first();
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
	@ApiOperation(value = "get result of TPCH Q4 using this model", notes = "Returns mongoDB document(s)", response = Document.class, responseContainer = "list")
	@ApiResponses(value = { @ApiResponse(code = 500, message = "internal server error !") })
	public Document getQ3Results() {

		AggregateIterable<Document> result;

		try {

			BsonDocument bsonQuery = BsonDocument
					.parse("{\"orderdate\": {\"$gte\": ISODate(\"2015-01-01T00:00:00.000Z\")},\"orderdate\": {\"$lt\": ISODate(\"2016-01-01T00:00:00.000Z\")}}");

			this.database.createCollection("normalized_q4_new_joined_orders");

			final MongoCollection<Document> normalized_q4_new_joined_orders = this.database
					.getCollection("normalized_q4_new_joined_orders");

			this.normalized_order.find(bsonQuery).forEach(
					new Block<Document>() {
						@Override
						public void apply(final Document order) {

							BsonDocument lineitemsBsonQuery = BsonDocument
									.parse("{\"_id\":{\"$in\":"
											+ new Gson().toJson(order
													.get("lineitems")) + "}}");

							order.put("lineitems", normalized_lineitem
									.find(lineitemsBsonQuery));

							normalized_q4_new_joined_orders.insertOne(order);
						}
					});

			String projectStringQuery = "{\"$project\":{\"orderdate\":1,\"orderpriority\":1,\"eq\":{\"$cond\":[{\"$lt\":[\"$lineitems.commitdate\",\"$lineitems.receiptdate\"]},0,1]}}}";

			String matchStringQuery = "{\"$match\":{\"eq\":{\"$eq\":1}}}";

			String groupStringQuery = "{\"$group\":{\"_id\":{\"o_orderpriority\":\"$orderpriority\"},\"order_count\":{\"$sum\":1}}}";

			String sortStringQuery = "{\"$sort\":{\"o_orderpriority\":1}}";

			// String out = "{\"$out\":\"out\"}";

			BsonDocument projectBsonQuery = BsonDocument
					.parse(projectStringQuery);

			BsonDocument matchBsonQuery = BsonDocument.parse(matchStringQuery);

			BsonDocument groupBsonQuery = BsonDocument.parse(groupStringQuery);

			BsonDocument sortBsonQuery = BsonDocument.parse(sortStringQuery);

			// BsonDocument outBson = BsonDocument.parse(out);

			LOGGER.info("matchBsonQuery is " + matchBsonQuery.toJson());

			LOGGER.info("groupBsonQuery is " + groupBsonQuery.toJson());

			LOGGER.info("sortBsonQuery is " + sortBsonQuery.toJson());

			ArrayList<Bson> aggregateQuery = new ArrayList<Bson>();

			aggregateQuery.add(projectBsonQuery);
			aggregateQuery.add(matchBsonQuery);
			// aggregateQuery.add(outBson);
			aggregateQuery.add(groupBsonQuery);
			aggregateQuery.add(sortBsonQuery);

			result = normalized_q4_new_joined_orders.aggregate(aggregateQuery);

			// LOGGER.info("result is " +result.first().toJson());

			return result.first();
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
