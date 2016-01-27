package org.neo4j.tpcH.utils;

import java.text.ParseException;
import java.util.logging.Logger;

import org.neo4j.graphdb.GraphDatabaseService;

public class InitializeData {

	private static final String CLASS_NAME = InitializeData.class.getName();
	private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

	GraphDatabaseService graphDb;

	public InitializeData(GraphDatabaseService graphDb) {

		this.graphDb = graphDb;
	}

	public void initialize() {

	}

}
