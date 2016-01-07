package org.mongoDB.tpcHQueries;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

public class NormalizedExampleModel {

	Document region = new Document();
	Document nation = new Document();
	Document customer = new Document();
	Document supplier = new Document();
	Document part = new Document();
	Document partsupp = new Document();
	Document lineitem = new Document();
	List<String> lineitems = new ArrayList<String>();
	Document order = new Document();

	DateFormat format = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss",
			java.util.Locale.ENGLISH);

	public NormalizedExampleModel() throws ParseException {

		region.append("_id", "18f228bc-a7d1-11e5-bf7f-feff819cdc9f")
				.append("name", "Texas").append("comment", "some text");

		nation.append("_id", "0aa770e6-a7d1-11e5-bf7f-feff819cdc9f")
				.append("name", "US").append("comment", "some text")
				.append("region", "18f228bc-a7d1-11e5-bf7f-feff819cdc9f");

		customer.append("_id", "f4005128-a7d0-11e5-bf7f-feff819cdc9f")
				.append("name", "Bilal").append("address", "Street 1")
				.append("phone", 1223456).append("acctbal", 212)
				.append("mktsegment", "some text")
				.append("comment", "some text")
				.append("nation", "0aa770e6-a7d1-11e5-bf7f-feff819cdc9f");

		part.append("_id", "75f28eda-a7d1-11e5-bf7f-feff819cdc9f")
				.append("name", "Tshirt").append("mfgr", "Boss")
				.append("brand", "Boss").append("type", "sport")
				.append("size", 40).append("container", "some text")
				.append("retailprice", 230).append("comment", "some text");

		supplier.append("_id", "968a0d3a-a7d1-11e5-bf7f-feff819cdc9f")
				.append("name", "Boss Supplier").append("address", "street 2")
				.append("phone", 212323).append("acctbal", 2933)
				.append("comment", "some text")
				.append("nation", "0aa770e6-a7d1-11e5-bf7f-feff819cdc9f");

		partsupp.append("_id", "62353e7e-a7d1-11e5-bf7f-feff819cdc9f")
				.append("availqty", 20).append("supplycost", 220)
				.append("comment", "some text")
				.append("part", "75f28eda-a7d1-11e5-bf7f-feff819cdc9f")
				.append("supplier", "968a0d3a-a7d1-11e5-bf7f-feff819cdc9f");

		lineitem.append("_id", "2ddbd282-a7d1-11e5-bf7f-feff819cdc9f")
				.append("quantity", 1).append("extendedprice", 200)
				.append("discount", 20).append("tax", 2)
				.append("returnflag", false).append("linestatus", "available")
				.append("shipdate", format.parse("2015-12-21 10:51:25"))
				.append("commitdate", format.parse("2015-12-21 10:51:25"))
				.append("receiptdate", format.parse("2015-12-21 10:51:25"))
				.append("shipinstruct", "some text").append("shipmode", "DHL")
				.append("comment", "some text")
				.append("partsupp", "62353e7e-a7d1-11e5-bf7f-feff819cdc9f");

		lineitems.add("2ddbd282-a7d1-11e5-bf7f-feff819cdc9f");

		order.append("_id", "7821ef4d-8e8c-46e0-950d-83c245b9bee8")
				.append("orderstatus", "Open").append("totalprice", 233)
				.append("orderdate", format.parse("2015-12-21 10:51:25"))
				.append("orderpriority", "High").append("clerk", "John")
				.append("shippriority", "High")
				.append("comment", "This is an order")
				.append("customer", "f4005128-a7d0-11e5-bf7f-feff819cdc9f")
				.append("lineitems", lineitems);
	}

	public Document getRegion() {
		return region;
	}

	public Document getNation() {
		return nation;
	}

	public Document getCustomer() {
		return customer;
	}

	public Document getSupplier() {
		return supplier;
	}

	public Document getPart() {
		return part;
	}

	public Document getPartsupp() {
		return partsupp;
	}

	public Document getLineitem() {
		return lineitem;
	}

	public Document getOrder() {
		return order;
	}

}
