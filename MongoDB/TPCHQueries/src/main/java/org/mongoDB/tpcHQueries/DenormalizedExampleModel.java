package org.mongoDB.tpcHQueries;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

public class DenormalizedExampleModel {

	Document region = new Document();
	Document nation = new Document();
	Document customer = new Document();
	Document supplier = new Document();
	Document part = new Document();
	Document partsupp = new Document();
	Document lineitem = new Document();
	List<Document> lineitems = new ArrayList<Document>();
	Document order = new Document();

	DateFormat format = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss",
			java.util.Locale.ENGLISH);

	public DenormalizedExampleModel() throws ParseException {

		region.append("_id", "18f228bc-a7d1-11e5-bf7f-feff819cdc9f")
				.append("name", "Texas").append("comment", "some text");

		nation.append("_id", "0aa770e6-a7d1-11e5-bf7f-feff819cdc9f")
				.append("name", "US").append("comment", "some text")
				.append("region", region);

		customer.append("_id", "f4005128-a7d0-11e5-bf7f-feff819cdc9f")
				.append("name", "Bilal").append("address", "Street 1")
				.append("phone", 1223456).append("acctbal", 212)
				.append("mktsegment", "some text")
				.append("comment", "some text").append("nation", nation);

		part.append("_id", "75f28eda-a7d1-11e5-bf7f-feff819cdc9f")
				.append("name", "Tshirt").append("mfgr", "Boss")
				.append("brand", "Boss").append("type", "sport")
				.append("size", 40).append("container", "some text")
				.append("retailprice", 230).append("comment", "some text");

		supplier.append("_id", "968a0d3a-a7d1-11e5-bf7f-feff819cdc9f")
				.append("name", "Boss Supplier").append("address", "street 2")
				.append("phone", 212323).append("acctbal", 2933)
				.append("comment", "some text").append("nation", nation);

		partsupp.append("_id", "62353e7e-a7d1-11e5-bf7f-feff819cdc9f")
				.append("availqty", 20).append("supplycost", 220)
				.append("comment", "some text").append("part", part)
				.append("supplier", supplier);

		lineitem.append("_id", "2ddbd282-a7d1-11e5-bf7f-feff819cdc9f")
				.append("quantity", 1).append("extendedprice", 200)
				.append("discount", 20).append("tax", 2)
				.append("returnflag", false).append("linestatus", "available")
				.append("shipdate", format.parse("2015-12-21 10:51:25"))
				.append("commitdate", format.parse("2015-12-21 10:51:25"))
				.append("receiptdate", format.parse("2015-12-21 10:51:25"))
				.append("shipinstruct", "some text").append("shipmode", "DHL")
				.append("comment", "some text").append("partsupp", partsupp);

		lineitems.add(lineitem);

		order.append("_id", "7821ef4d-8e8c-46e0-950d-83c245b9bee8")
				.append("orderstatus", "Open").append("totalprice", 233)
				.append("orderdate", format.parse("2015-12-21 10:51:25"))
				.append("orderpriority", "High").append("clerk", "John")
				.append("shippriority", "High")
				.append("comment", "This is an order")
				.append("customer", customer).append("lineitems", lineitems);
	}

	public Document getOrder() {
		return order;
	}

}
