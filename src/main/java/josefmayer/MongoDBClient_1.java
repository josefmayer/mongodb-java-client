package josefmayer;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;

public class MongoDBClient_1{

    public static void main (String [] args){

        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

        MongoDatabase database = mongoClient.getDatabase("mydb");

        MongoCollection<Document> collection = database.getCollection("test1");

        insertDoc(collection);
        insertDocList(collection);

        countDocuments(collection);
        query_first(collection);
        query_filter_1(collection);
        query_filter_block(collection);

        printCollection(collection);

        updateSingleDocument(collection);
        updateMultipleDocuments(collection);

        printCollection(collection);
    }

    static void insertDoc(MongoCollection<Document> collection){
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));
        collection.insertOne(doc);
    }

    static void insertDocList(MongoCollection collection){
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 20; i++) {
            documents.add(new Document("i", i));
        }
        collection.insertMany(documents);

    }

    static void updateSingleDocument(MongoCollection<Document> collection){
        collection.updateOne(eq("i", 14), new Document("$set", new Document("i", 110)));
    }

    static void updateMultipleDocuments(MongoCollection<Document> collection){
        UpdateResult updateResult = collection.updateMany(lt("i", 10), inc("i", 100));
        System.out.println(updateResult.getModifiedCount());
    }

    static void countDocuments(MongoCollection<Document> collection){
        System.out.println("Documents in collection: " + collection.count());
    }

    static void query_first(MongoCollection<Document> collection){
        Document myDoc = collection.find().first();
        System.out.println(myDoc.toJson());
    }

    static void query_filter_1(MongoCollection<Document> collection){
        Document myDoc = collection.find(eq("i", 7)).first();
        System.out.println(myDoc.toJson());
    }

    static void query_filter_block(MongoCollection<Document> collection){
        Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        collection.find(gt("i", 15)).forEach(printBlock);
    }

    static void printCollection(MongoCollection<Document> collection){
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

    }
}