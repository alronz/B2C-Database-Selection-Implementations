package org.mongoDB.tpcHQueries.utils;

import java.text.ParseException;
import java.util.logging.Logger;

import org.bson.Document;
import org.mongoDB.tpcHQueries.DenormalizedExampleModel;
import org.mongoDB.tpcHQueries.MixedExampleModel;
import org.mongoDB.tpcHQueries.NormalizedExampleModel;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class InitializeData {

	private static final String CLASS_NAME = InitializeData.class.getName();
	private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

	private MongoClient mongoClient;
	private MongoDatabase database;

	private MongoCollection<Document> denormalized_order;

	private MongoCollection<Document> mixed_customer;
	private MongoCollection<Document> mixed_supplier;
	private MongoCollection<Document> mixed_partsupp;
	private MongoCollection<Document> mixed_order;

	private MongoCollection<Document> normalized_customer;
	private MongoCollection<Document> normalized_supplier;
	private MongoCollection<Document> normalized_partsupp;
	private MongoCollection<Document> normalized_order;
	private MongoCollection<Document> normalized_region;
	private MongoCollection<Document> normalized_nation;
	private MongoCollection<Document> normalized_part;
	private MongoCollection<Document> normalized_lineitem;

	public InitializeData(MongoClient mongoClient) {

		this.mongoClient = mongoClient;

		this.database = this.mongoClient.getDatabase("mydb");

	}

	public void initialize() {
		insertDenormalizedModel();

		insertNormalizedModel();

		insertMixedModel();
	}

	void insertDenormalizedModel() {
		DenormalizedExampleModel denormalizedExampleModel = null;
		try {
			denormalizedExampleModel = new DenormalizedExampleModel();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (this.database.getCollection("denormalized_order") == null) {
			this.database.createCollection("denormalized_order");
		}

		denormalized_order = this.database.getCollection("denormalized_order");

		denormalized_order.insertOne(denormalizedExampleModel.getOrder());

	}

	void insertNormalizedModel() {
		NormalizedExampleModel normalizedExampleModel = null;
		try {
			normalizedExampleModel = new NormalizedExampleModel();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (this.database.getCollection("normalized_customer") == null) {
			this.database.createCollection("normalized_customer");
		}
		if (this.database.getCollection("normalized_supplier") == null) {
			this.database.createCollection("normalized_supplier");
		}
		if (this.database.getCollection("normalized_partsupp") == null) {
			this.database.createCollection("normalized_partsupp");
		}
		if (this.database.getCollection("normalized_order") == null) {
			this.database.createCollection("normalized_order");
		}
		if (this.database.getCollection("normalized_region") == null) {
			this.database.createCollection("normalized_region");
		}
		if (this.database.getCollection("normalized_nation") == null) {
			this.database.createCollection("normalized_nation");
		}
		if (this.database.getCollection("normalized_part") == null) {
			this.database.createCollection("normalized_part");
		}
		if (this.database.getCollection("normalized_lineitem") == null) {
			this.database.createCollection("normalized_lineitem");
		}

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

		normalized_customer.insertOne(normalizedExampleModel.getCustomer());
		normalized_supplier.insertOne(normalizedExampleModel.getSupplier());
		normalized_partsupp.insertOne(normalizedExampleModel.getPartsupp());
		normalized_order.insertOne(normalizedExampleModel.getOrder());
		normalized_region.insertOne(normalizedExampleModel.getRegion());
		normalized_nation.insertOne(normalizedExampleModel.getNation());
		normalized_part.insertOne(normalizedExampleModel.getPart());
		normalized_lineitem.insertOne(normalizedExampleModel.getLineitem());
	}

	void insertMixedModel() {
		MixedExampleModel mixedExampleModel = null;
		try {
			mixedExampleModel = new MixedExampleModel();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (this.database.getCollection("mixed_customer") == null) {
			this.database.createCollection("mixed_customer");
		}
		if (this.database.getCollection("mixed_supplier") == null) {
			this.database.createCollection("mixed_supplier");
		}
		if (this.database.getCollection("mixed_partsupp") == null) {
			this.database.createCollection("mixed_partsupp");
		}
		if (this.database.getCollection("mixed_order") == null) {
			this.database.createCollection("mixed_order");
		}

		mixed_customer = this.database.getCollection("mixed_customer");
		mixed_supplier = this.database.getCollection("mixed_supplier");
		mixed_partsupp = this.database.getCollection("mixed_partsupp");
		mixed_order = this.database.getCollection("mixed_order");

		mixed_customer.insertOne(mixedExampleModel.getCustomer());
		mixed_supplier.insertOne(mixedExampleModel.getSupplier());
		mixed_partsupp.insertOne(mixedExampleModel.getPartsupp());
		mixed_order.insertOne(mixedExampleModel.getOrder());
	}

}
