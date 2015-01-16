package com.hcm.mongodb.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
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

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class MongoDBFileDemo {
	
	private static final Logger logger = LoggerFactory.getLogger(MongoDBFileDemo.class);
	
	private static final String FILE_NAME = "filename";
	private static final String DEFAULT_DB_NAME = "mongo-demo";
	
	private Mongo mongo = null;
	
	@Before
	public void init() {
		try {
			mongo =  new MongoClient("127.0.0.1", 27017);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@After
	public void destory() {
		if (mongo != null) {
			mongo.close();
		}
	}
	
	private GridFS getGridFS() {
		return new GridFS(getDB());
	}
	
	private GridFS getGridFS(String dbName) {
		return new GridFS(getDB());
	}
	
	private DB getDB(String dbName) {
		if (StringUtils.isNotEmpty(dbName)) {
			return getDB(dbName);
		}
		
		return getDB(DEFAULT_DB_NAME);
	}

	private DB getDB() {
		return getDB(DEFAULT_DB_NAME);
	}

	/**
	 * 保存文件
	 *
	 * @param dbName
	 * @param byteFile
	 * @param fileName
	 * @param fileType
	 * @return
	 */
	public String saveFile(String dbName, byte[] byteFile, String fileName, String fileType) {
		if (StringUtils.isEmpty(fileName)) {
			return null;
		}
		GridFS fs = getGridFS(dbName);
		GridFSInputFile dbFiel = null;
		try {
			dbFiel = fs.createFile(byteFile);
			dbFiel.setContentType(fileType);
			dbFiel.setFilename(fileName);
			dbFiel.put(FILE_NAME, fileName);
			dbFiel.save();
		} catch (Exception e) {
			logger.error("", e);
		}
		
		return dbFiel.getId().toString();
	}
	
	/**
	 * 保存文件
	 *
	 * @param dbName
	 * @param fileName
	 * @param fileType
	 * @return
	 */
	public String saveFile(String dbName, String fileName, String fileType) {
		if (StringUtils.isEmpty(fileName)) {
			return null;
		}
		String fName = fileName.substring(fileName.lastIndexOf("/") + 1);
		GridFS fs = getGridFS();
		GridFSInputFile dbFile = null;
		try {
			dbFile = fs.createFile(new File(fileName));
			dbFile.setFilename(fName);
			dbFile.setContentType(fileType);
			dbFile.put(FILE_NAME, fName);
			dbFile.save();
		} catch (IOException e) {
			logger.error("", e);
		}
		return dbFile.getId().toString();
	}
	
	private byte[] file2Bytes(File file) {
		FileInputStream fis = null;
		byte[] byteFile = null;
		try {
			fis = new FileInputStream(file);
			int lenth = fis.available();
			byteFile = new byte[lenth];
			int temp = 0, i = 0;
			while ((temp = fis.read()) != -1) {
				byteFile[i] = (byte) temp;
				i++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return byteFile;
	}
	
	/**
	 * 按fileId获取文件
	 *
	 * @param dbName
	 * @param fileId
	 * @return
	 */
	public GridFSDBFile getFileById(String dbName, String fileId) {
		GridFS fs = getGridFS(dbName);
		DBObject query = new BasicDBObject("_id", new ObjectId(fileId));
		GridFSDBFile dbFile = fs.findOne(query);
		return dbFile;
	}
	
	/**
	 * 按fileName获取文件
	 *
	 * @param dbName
	 * @param fileName
	 * @return
	 */
	public GridFSDBFile getFileByName(String dbName, String fileName) {
		GridFS fs = getGridFS(dbName);
		DBObject query = new BasicDBObject(FILE_NAME, fileName);
		GridFSDBFile dbFile = fs.findOne(query);
		return dbFile;
	}
	
	/**
	 * 按fielId删除文件
	 *
	 * @param dbName
	 * @param fileId
	 */
	public void deleteFileById(String dbName, String fileId) {
		if (fileId == null) {
			return;
		}
		GridFS fs = getGridFS(dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(fileId));
		if (dbFile == null) {
			return;
		}
		fs.remove(dbFile);
	}
	
	/**
	 * 按fileName删除文件
	 *
	 * @param dbName
	 * @param fileName
	 */
	public void deleteFileByName(String dbName, String fileName) {
		if (StringUtils.isEmpty(fileName)) {
			return;
		}
		GridFS fs = getGridFS(dbName);
		DBObject dbObject = new BasicDBObject(FILE_NAME, fileName);
		GridFSDBFile dbFile = fs.findOne(dbObject);
		if (dbFile == null) {
			return;
		}
		fs.remove(dbFile);
	}
	
	/**
	 * 按fielId读取文件
	 * 
	 * @param dbName
	 * @param fileId
	 * @return
	 */
	public byte[] readFileById(String dbName, String fileId) {
		if (StringUtils.isEmpty(fileId)) {
			return null;
		}
		GridFS fs = getGridFS(dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(fileId));
		if (dbFile == null) {
			return null;
		}
		
		return readFile(dbFile);
	}
	
	/**
	 * 按fielName读取文件
	 *
	 * @param dbName
	 * @param fileName
	 * @return
	 */
	public byte[] readFileByName(String dbName, String fileName) {
		if (StringUtils.isEmpty(fileName)) {
			return null;
		}
		
		GridFS fs = getGridFS(dbName);
		DBObject dbObject = new BasicDBObject(FILE_NAME, fileName);
		GridFSDBFile dbFile = fs.findOne(dbObject);
		if (dbFile == null) {
			return null;
		}
		
		return readFile(dbFile);
	}
	
	/**
	 * GridFSDBFile 转存成 byte[]
	 *
	 * @param dbFile
	 * @return
	 */
	private byte[] readFile(GridFSDBFile dbFile) {
		if (dbFile == null) {
			return null;
		}
		InputStream is = null;
		byte[] resultByte = null;
		try {
			is = dbFile.getInputStream();
			int lenth = (int) dbFile.getLength();
			resultByte = new byte[lenth];
			int temp = 0, i = 0;
			while ((temp = is.read()) != -1 && i < lenth) {
				resultByte[i] = (byte) temp;
				i++;
			}
		} catch (IOException e) {
			logger.error("", e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					logger.error("", e);
				}
			}
		}

		return resultByte;
	}
	
	/**
	 * 按fileId读文件保存到本地
	 *
	 * @param dbName
	 * @param fileId
	 * @param localFileName
	 */
	public void readFileToLocal(String dbName, String fileId, String localFileName) {
		if (StringUtils.isEmpty(fileId)) {
			return;
		}
		
		GridFS fs = getGridFS(dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(fileId));
		if (dbFile == null) {
			return;
		}
		File file = new File(localFileName);
		try {
			dbFile.writeTo(file);
		} catch (IOException e) {
			logger.error("", e);
		}
	}
	
	/**
	 * 按finlName读文件保存到本地
	 *
	 * @param dbName
	 * @param fileName
	 * @param localFileName
	 */
	public void readFileToLocalByName(String dbName, String fileName, String localFileName) {
		if (StringUtils.isEmpty(fileName)) {
			return;
		}
		GridFS fs = getGridFS(dbName);
		GridFSDBFile dbFile = fs.findOne(new BasicDBObject(FILE_NAME, fileName));
		if (dbFile == null) {
			return;
		}
		File file = new File(localFileName);
		try {
			dbFile.writeTo(file);
		} catch (IOException e) {
			logger.error("", e);
		}
	}
	
	/**
	 * 根据fielId获得文件名称
 	 *
	 * @param dbName
	 * @param fileId
	 * @return
	 */
	public String getFileName(String dbName, String fileId) {
		if (StringUtils.isEmpty(fileId)) {
			return null;
		}
		
		GridFS fs = getGridFS(dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(fileId));
		if (dbFile == null) {
			return null;
		}
		return dbFile.get(FILE_NAME).toString();
	}
	
	/**
	 * 更新文件
	 *
	 * @param dbName
	 * @param byteFile
	 * @param fileId
	 * @param fileName
	 * @param fileType
	 * @return
	 */
	public String updateFile(String dbName, byte[] byteFile, String fileId, String fileName, String fileType) {
		if (byteFile == null || StringUtils.isEmpty(fileId)) {
			return null;
		}
		GridFS fs = getGridFS(dbName);
		GridFSInputFile dbFile = fs.createFile(byteFile);
		// 设置文件Id
		dbFile.setId(new ObjectId(fileId));
		
		dbFile.setContentType(fileType);
		dbFile.setFilename(fileName);
		dbFile.put("fileName", fileName);
		// 删除
		deleteFileById(dbName, fileId);
		// 保存
		dbFile.save();
		return dbFile.getId().toString();
	}
	
	/**
	 * 获得最后更新时间
	 *
	 * @param dbName
	 * @param fileId
	 * @return
	 */
	public Date readUpdateTime(String dbName, String fileId) {
		if (fileId == null) {
			return null;
		}
		GridFS fs = getGridFS(dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(fileId));
		if (dbFile == null) {
			return null;
		}
		try {
			return dbFile.getUploadDate();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 带条件查询
	 *
	 * @param dbName
	 * @param condition
	 * @return
	 */
	public List<GridFSDBFile> queryWithCondition(String dbName, Map<String, String> condition) {
		if (condition == null) {
			return null;
		}
		GridFS fs = getGridFS(dbName);
		DBObject ob = new BasicDBObject();
		for (String key : condition.keySet()) {
			ob.put(key, condition.get(key));
		}
		List<GridFSDBFile> dbFiles = fs.find(ob);
		return dbFiles;
	}
	
	@Test
	public void getFileList() {
		GridFS fs = new GridFS(getDB());
		Cursor cursor = fs.getFileList();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	@Test
	public void testSaveFile1() {
		String fileName = "test1.properties";
		String fileType = "java";
		String id = saveFile(DEFAULT_DB_NAME, fileName, fileType);
		System.out.println(id);
	}
	
	@Test
	public void testSaveFile2() {
		String fileName = "log4j.properties";
		String fileType = "properties";
		File file = new File("src/log4j.properties");
		String id = saveFile(DEFAULT_DB_NAME, file2Bytes(file), fileName, fileType);
		System.out.println(id);
	}
	
	@Test
	public void testFindOne() {
		GridFSDBFile dbFile = getFileById(DEFAULT_DB_NAME, "54b6239b63c8538e4ff050d7");
		System.out.println(dbFile);
		GridFSDBFile dbFile2 = getFileByName(DEFAULT_DB_NAME, "log4j.properties");
		System.out.println(dbFile2);
	}
	
	@Test
	public void testDelete() {
		deleteFileById(DEFAULT_DB_NAME, "54b612a363c86e415d5c2742");
		deleteFileByName(DEFAULT_DB_NAME, "MongoTest.java");
	}
	
	@Test
	public void testRead() {
		// 读文件
		byte[] data = readFileById(DEFAULT_DB_NAME, "54b61dd163c8f5061b147953");
		System.out.println(data.length/1024 + "KB");
		
		byte[] data2 = readFileByName(DEFAULT_DB_NAME, "log4j.properties");
		System.out.println(data2.length/1024 + "KB");
		// 读mongodb文件到本地
		String localFileName = "test1.properties";
		readFileToLocal(DEFAULT_DB_NAME, "54b61dd163c8f5061b147953", localFileName);
		String localFileName2 = "test2.properties";
		readFileToLocalByName(DEFAULT_DB_NAME, "log4j.properties", localFileName2);
	}
	
	@Test
	public void testUpdate() {
		// 54b6239b63c8538e4ff050d7
		String id = updateFile(DEFAULT_DB_NAME, file2Bytes(new File("test1.properties")), "54b6239b63c8538e4ff050d7", "test2.properties", "properties");
		System.out.println(id);
		System.out.println(readUpdateTime(DEFAULT_DB_NAME, "54b6239b63c8538e4ff050d7"));
	}
	
	@Test
	public void testQuery() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("fileName", "test2.properties");
		List<GridFSDBFile> dbFileList = queryWithCondition(DEFAULT_DB_NAME, map);
		if (dbFileList != null && dbFileList.size() > 0) {
			for (GridFSDBFile dbFile : dbFileList) {
				System.out.println(dbFile);
			}
		}
	}
	
}


