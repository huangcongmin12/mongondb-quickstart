package com.hcm.mongodb.demo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class MongoDBDemo {

	private static final Logger logger = LoggerFactory.getLogger(MongoDBFileDemo.class);
	
	private static final String DEFAULT_DB_NAME = "mongo-demo";
	
	private  Mongo mongo = null;
	
	@Before
	public void init() {
		try {
			mongo = new MongoClient("127.0.0.1", 27017);
		} catch (UnknownHostException e) {
			logger.error("", e);
		}
	}

	@After
	public void destory() {
		if (mongo != null) {
			mongo.close();
		}
	}
	
	public  DBCollection getCollection(String dbName, String collectionName) {
		return getDB(dbName).getCollection(collectionName);
	}

	public DBCollection getCollection(String collectionName) {
		return getDB().getCollection(collectionName);
	}

	private DB getDB(String dbName) {
		if (StringUtils.isNotEmpty(dbName)) {
			return mongo.getDB(dbName);
		}
		return mongo.getDB(DEFAULT_DB_NAME);
	}

	private DB getDB() {
		return mongo.getDB(DEFAULT_DB_NAME);
	}
	
	@Test
	public void add() {
		DBCollection users = getCollection("user");
		
		DBObject obj = new BasicDBObject();
		obj.put("name", "Jiang");
		obj.put("age", "18");
		obj.put("sex", "女");
		
		users.save(obj);
		
	}
	
	@Test
	public void insert() {
		DBCollection users = getCollection("user");
		
		DBObject obj = new BasicDBObject();
		obj.put("name", "hello");
		obj.put("age", "18");
		obj.put("sex", "男");
		
		users.insert(obj);
		
	}
	
	@Test
	public void inserts() {
		
		DBCollection users = getCollection("user");
		
		List<DBObject> objs = new ArrayList<DBObject>();
		
		DBObject obj1 = new BasicDBObject();
		obj1.put("name", "wqqe");
		obj1.put("age", "19");
		obj1.put("sex", "女");
		
		DBObject obj2 = new BasicDBObject();
		obj2.put("name", "aaa");
		obj2.put("age", "19");
		obj2.put("sex", "女");
		
		objs.add(obj1);
		objs.add(obj2);
		
		users.insert(objs);
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "kkkk");
		map.put("age", 9);
		map.put("sex", "XXXooo");
		
		users.insert(new BasicDBObject("name", "hello"), new BasicDBObject(map));
	}
	
	@Test
	public void queryAll() {
		DBCollection users = getCollection("user");
		
		DBCursor cursor = users.find();
		
//		while (cursor.hasNext()) {
//			System.out.println(cursor.next());
//		}
		
		System.out.println(JSON.serialize(cursor));
	}
	
	
	@Test
	public void remove() {
		DBCollection users = getCollection("user");
		DBObject o = new BasicDBObject();
		//o.put("_id", new ObjectId("54990c38363078af81f3d950"));
		o.put("firstName", "hello");
		users.remove(o);
	}
	
	@Test
	public void update() {
		DBCollection users = getCollection("user");
//		WriteResult result = users.update(
//				new BasicDBObject("_id", new ObjectId("54990f5836300c9ca6bd0e19")), 
//				new BasicDBObject("age", 99));
		
		WriteResult result = users.update(
				new BasicDBObject("_id", new ObjectId("54990f5836300c9ca6bd0e19")), 
				new BasicDBObject("age", 121), true, false);

		System.out.println(result.getN());
	}
	

	/**
	 * OR多条件查询
	 */
	public void findByORQuery(int lte, int gt, long longData) {
		DBCollection coll = getCollection("user");
		BasicDBObject query = new BasicDBObject();
		BasicDBObject longdata = new BasicDBObject("longData", longData);
		BasicDBObject intdata = new BasicDBObject("intData", new BasicDBObject().append("$gt", gt).append("$lte", lte));
		BasicDBList cond = new BasicDBList();
		cond.add(longdata);
		cond.add(intdata);
		query.put("$or", cond);
		DBCursor cur = coll.find(query);
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	/**
	 * IN查询
	 */
	public void findByINQuery(int value1, int value2) {
		DBCollection coll = getCollection(null, "user");
		BasicDBObject query = new BasicDBObject();
		BasicDBList cond = new BasicDBList();
		cond.add(value1);
		cond.add(value2);
		query.put("intData", new BasicDBObject("$in", cond));
		DBCursor cur = coll.find(query);
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	/**
	 * NOT查询
	 */
	public void findByNotQuery(int value1, int value2) {
		DBCollection coll = getCollection(null, "user");
		BasicDBObject query = new BasicDBObject();
		BasicDBList cond = new BasicDBList();
		cond.add(value1);
		cond.add(value2);
		query.put("intData", new BasicDBObject("$nin", cond));
		System.out.println("Count:" + coll.find(query).count());
	}

	/**
	 * 获取结果集第一条
	 */
	public void fetchFirstQuery(int value1, int value2) {
		DBCollection coll = getCollection(null, "user");
		BasicDBList cond = new BasicDBList();
		cond.add(value1);
		cond.add(value2);
		BasicDBObject query = new BasicDBObject().append("intData", new BasicDBObject("$nin", cond));
		System.out.println(coll.findOne(query));
	}

	/**
	 * 查询文档部分列
	 */
	public void querySomeKey() {
		DBCollection coll = getCollection("user");
		DBCursor cur = coll.find(new BasicDBObject(), new BasicDBObject("intData", true));
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	/**
	 * 查询内嵌文档
	 */
	public void queryInnerDocument() {
		DBCollection coll = getCollection("user");
		BasicDBObject map = new BasicDBObject();
		map.put("innertype", "string");
		map.put("innerContent", "string0");
		DBCursor cur = coll.find(new BasicDBObject("documentData", map));
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	/**
	 * 查询内嵌部分文档
	 */
	public void querySubInnerDocument() {
		DBCollection coll = getCollection("user"); 
		DBCursor cur = coll.find(new BasicDBObject("documentData.innerContent", "string0"));
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	/**
	 * 查询分页文档
	 */
	public void queryByPage(int skipNum, int pageNum) {
		DBCollection coll = getCollection("user");
		DBCursor cur = coll.find().skip(skipNum).limit(pageNum);
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	/**
	 * 查询文档某列是否存在
	 */
	public void queryExists() {
		DBCollection coll = getCollection("user");
		DBCursor cur = coll.find(new BasicDBObject("longData", new BasicDBObject("$exists", true)));
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	/**
	 * 查询文档排序
	 */
	public void sortDocument() {
		DBCollection coll = getCollection("user");
		DBCursor cur = coll.find().sort(new BasicDBObject("intData", -1));// 1:asc
																			// /
																			// -1:desc
		while (cur.hasNext()) {
			System.out.println(cur.next());
		}
	}

	/**
	 * 获取根据某元素做distinct查询.
	 */
	@SuppressWarnings("unchecked")
	public void distinctKey() {
		DBCollection coll = getCollection("user");
		List<String> list = coll.distinct("documentData.innertype");
		for (String str : list) {
			System.out.println(str);
		}
	}

	/**
	 * 创建唯一索引
	 */
	public void createIndexes() {
		DBCollection coll = getCollection("user");
		BasicDBObject index = new BasicDBObject();
		index.put("intData", 1);// 1:asc / -1:desc
		index.put("unique", true);// 唯一索引
		coll.createIndex(index);
	}

	/**
	 * 查询索引信息
	 */
	public void getIndexes() {
		DBCollection coll = getCollection("user");
		List<DBObject> indexInfo = coll.getIndexInfo();
		System.out.println(indexInfo);
	}

	/**
	 * 删除索引信息
	 */
	public void dropIndexes() {
		DBCollection coll = getCollection("user");
		// 删除的索引必须跟创建的索引名称\排序\是否唯一都相同才能删除
		BasicDBObject index = new BasicDBObject();
		index.put("intData", 1);
		index.put("unique", true);
		coll.dropIndex(index);
	}

}
