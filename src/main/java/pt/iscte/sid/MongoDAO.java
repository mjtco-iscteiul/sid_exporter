import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lte;

import org.bson.Document;
import org.bson.types.BSONTimestamp;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class MongoDAO {

	public MongoClient connect(String[] properties, boolean valid) {
		MongoClient mongoClient = null;
		if(valid) {
			String connection = "mongodb://"+properties[2]+":"+properties[0]+"@"+properties[6]+":"+properties[1]+"/SensorData";
			mongoClient = MongoClients.create(connection);
		}else {
			mongoClient = MongoClients.create("mongodb://UExportador:exportador@172.17.1.245:27017/SensorData");
		}
		return mongoClient;
	}

	public void disconnect(MongoClient mongoClient) {
		mongoClient.close();
	}

	public MongoDatabase databaseConnect(MongoClient mongoClient, String[] properties, boolean valid) {
		MongoDatabase database = null;
		if(valid) {
			database = mongoClient.getDatabase(properties[4]);
		}else {
			database = mongoClient.getDatabase("SensorData");
		}
		return database;
	}

	public FindIterable<Document> getRecentResults(MongoDatabase database, BSONTimestamp timestamp, BSONTimestamp timestampAtual, String[] properties, boolean valid) {
		FindIterable<Document> all = null;
		if(valid) {
			MongoCollection<Document> collection = database.getCollection(properties[5]);
			all = collection.find(and(gt("created_at", timestamp),lte("created_at", timestampAtual)));
		}else {
			MongoCollection<Document> collection = database.getCollection("humidityTemperature");
			all = collection.find(and(gt("created_at", timestamp),lte("created_at", timestampAtual)));
		}
		return all;
	}

}