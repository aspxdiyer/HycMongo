using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Driver.Builders;

namespace HycMongo
{
  public class Configuration
  {
    // 静态字段
    private static MongoClient _MongoClient;
    private static MongoServer _MongoServer;
    private static MongoDatabase _MongoDatabase;
    private static MongoCollection<BsonDocument> _MongoCollection;
    private static MongoServerSettings _MongoServerSettings;

    private static string _database;//数据库名称

    private static IMongoCollectionOptions options = CollectionOptions.SetAutoIndexId(false).SetMaxSize(10000000).SetMaxDocuments(1000);

    //构析
    private Configuration()
    {
      var connectionString = Environment.GetEnvironmentVariable("CSharpDriverTestsConnectionString")
        ?? "mongodb://localhost/?w=1";

      var mongoUrl = new MongoUrl(connectionString);
      var clientSettings = MongoClientSettings.FromUrl(mongoUrl);
      if (!clientSettings.WriteConcern.Enabled)
      {
        clientSettings.WriteConcern.W = 1; // ensure WriteConcern is enabled regardless of what the URL says
      }

      _MongoClient = new MongoClient(clientSettings);
      _MongoServer = _MongoClient.GetServer();
      _MongoDatabase = _MongoServer.GetDatabase(_database);
      _MongoCollection = _MongoDatabase.GetCollection("testcollection");
    }


    // public static properties
    /// <summary>
    /// Gets the test client.
    /// </summary>
    public static MongoClient MongoClient
    {
      get { return _MongoClient; }
    }

    /// <summary>
    /// Gets the test collection.
    /// </summary>
    public static MongoCollection<BsonDocument> MongoCollection
    {
      get { return _MongoCollection; }
    }

    /// <summary>
    /// Gets the test database.
    /// </summary>
    public static MongoDatabase MongoDatabase
    {
      get { return _MongoDatabase; }
    }

    /// <summary>
    /// Gets the test server.
    /// </summary>
    public static MongoServer MongoServer
    {
      get { return _MongoServer; }
    }

    // public static methods
    /// <summary>
    /// Gets the test collection with a default document type of T.
    /// </summary>
    /// <typeparam name="T">The default document type.</typeparam>
    /// <returns>The collection.</returns>
    public static MongoCollection<T> GetMongoCollection<T>()
    {
      return _MongoDatabase.GetCollection<T>(_MongoCollection.Name);
    }


  }
}
