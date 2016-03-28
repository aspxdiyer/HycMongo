using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace HycMongo
{
  public class MongoHelper<T> where T : class,new()
  {

    private MongoServer server = null;

    private string dataName = "";

    private string tableName = "";

    private IMongoCollectionOptions options = CollectionOptions.SetAutoIndexId(false).SetMaxSize(10000000).SetMaxDocuments(1000);

    public MongoHelper(string dataName, string tableName)
    {
      this.dataName = dataName;
      this.tableName = tableName;
      server = MongoServer.Create("mongodb://localhost/");
    }

    public MongoHelper(string dataName, string tableName, MongoServerSettings settings = null)
    {
      this.dataName = dataName;
      this.tableName = tableName;
      if (settings != null)
        server = new MongoServer(settings);
      else
        server = MongoServer.Create("mongodb://localhost/");
    }

    public MongoHelper(string connectionString)
    {
      var builder = new MongoUrlBuilder(connectionString);
      var url = builder.ToMongoUrl();
      var settings = MongoServerSettings.FromUrl(url);
      //settings.Credentials = new[] { MongoCredential.CreateMongoCRCredential(dataName, username, password) };
      server = new MongoServer(settings);
      this.dataName = builder.DatabaseName;
    }

    public MongoHelper(string dataName, string tableName, MongoServerSettings settings = null, IMongoCollectionOptions options = null)
    {
      this.dataName = dataName;
      this.tableName = tableName;
      if (settings != null)
        server = new MongoServer(settings);
      else
        server = MongoServer.Create("mongodb://localhost/");
      if (options != null)
        this.options = options;
    }

    /// <summary>
    /// 表名
    /// </summary>
    public string TableName
    {
      get { return this.tableName; }
      set { this.tableName = value; }
    }

    private MongoCollection<T> Open()
    {
      server.Connect();
      var database = server.GetDatabase(dataName);//[dataName];//数据库
      //MongoCollection<T> collection = database[tableName];//表
      MongoCollection<T> collection = database.GetCollection<T>(tableName);//表
      if (!collection.Exists())
      {
        database.CreateCollection(tableName, options);
      }
      return collection;
    }

    private void Close()
    {
      server.Disconnect();
    }

    /// <summary>
    /// 插入
    /// </summary>
    /// <param name="model"></param>
    public void Save(T model)
    {
      MongoCollection<T> collection = Open();
      try
      {
        collection.Save(model, SafeMode.True);
      }
      finally
      {
        Close();
      }
    }

    /// <summary>
    /// 更新
    /// </summary>
    /// <param name="query">搜索条件（例：Query.EQ("_id", id);）</param>
    /// <param name="update">更新（例：var d1 = new D { X = 1 };update = Update.AddToSetWrapped("InnerObjects", d1);）</param>
    public void Update(IMongoQuery query, UpdateBuilder update)
    {
      MongoCollection<T> collection = Open();
      try
      {
        collection.Update(query, update);
      }
      finally
      {
        Close();
      }
    }

    /// <summary>
    /// 删除
    /// </summary>
    /// <param name="query"></param>
    public void Remove(IMongoQuery query)
    {
      MongoCollection<T> collection = Open();
      try
      {
        collection.Remove(query, SafeMode.True);
      }
      finally
      {
        Close();
      }
    }

    /// <summary>
    /// 查询
    /// </summary>
    /// <param name="query">条件 QueryComplete q = null;q = Query.And(Query.EQ("FromUserName", user));</param>
    /// <returns></returns>
    public MongoCursor<T> FindList(IMongoQuery query = null)
    {
      MongoCollection<T> collection = Open();
      try
      {
        if (query == null)
          return collection.FindAll();
        else
          return collection.Find(query);
      }
      finally
      {
        Close();
      }
    }

    public MongoCursor<T> FindByPage(IMongoQuery query = null, IMongoSortBy sort = null, Int32 PageIndex = 0, Int32 PageSize = 0)
    {
      MongoCollection<T> collection = Open();
      sort = sort ?? new SortByDocument { };
      PageSize = (PageSize == 0) ? 1 : PageSize;
      try
      {
        if (PageIndex < 1)
          return ((query == null) ? collection.FindAll() : collection.Find(query)).SetSortOrder(sort);
        else
          return ((query == null) ? collection.FindAll() : collection.Find(query)).SetSortOrder(sort).SetSkip((PageIndex - 1) * PageSize).SetLimit(PageSize);
      }
      finally
      {
        Close();
      }
    }


    /// <summary>
    /// 统计
    /// </summary>
    /// <param name="query">条件 QueryComplete q = null;q = Query.And(Query.EQ("FromUserName", user));</param>
    /// <returns></returns>
    public long FindCount(IMongoQuery query = null)
    {
      MongoCollection<T> collection = Open();
      try
      {
        if (query == null)
          return collection.Count();//.FindAll().Count();
        else
          return collection.Count(query);
      }
      finally
      {
        Close();
      }
    }

    /// <summary>
    /// 查询实体
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    public T TryFind(IMongoQuery query = null)
    {
      MongoCollection<T> collection = Open();
      try
      {
        if (query == null)
          return null;
        else
          return collection.FindOne(query);
      }
      finally
      {
        Close();
      }
    }

    /// <summary>
    /// SqlHelper.CreateIndex(IndexKeys.Ascending("Name"));//创建索引
    /// </summary>
    /// <param name="Keys"></param>
    public void CreateIndex(IMongoIndexKeys Keys)
    {
      MongoCollection<T> collection = Open();
      try
      {
        collection.CreateIndex(Keys);//创建索引
      }
      finally
      {
        Close();
      }
    }

    /// <summary>
    /// SqlHelper.CreateIndex(new string[] { "timeStamp" });//创建索引
    /// </summary>
    /// <param name="Keys"></param>
    public void CreateIndex(string[] Keys)
    {
      MongoCollection<T> collection = Open();
      try
      {
        collection.CreateIndex(Keys);//创建索引
      }
      finally
      {
        Close();
      }
    }

    public void RemoveIndex(IMongoIndexKeys Keys = null)
    {
      MongoCollection<T> collection = Open();
      try
      {
        if (Keys == null)
          collection.DropAllIndexes();//删除全部索引
        else
          collection.DropIndex(Keys);//删除索引
      }
      finally
      {
        Close();
      }
    }

    public void RemoveIndex(string[] Keys = null)
    {
      MongoCollection<T> collection = Open();
      try
      {
        if (Keys == null)
          collection.DropAllIndexes();//删除全部索引
        else
          collection.DropIndex(Keys);//删除索引
      }
      finally
      {
        Close();
      }
    }

  }
}
