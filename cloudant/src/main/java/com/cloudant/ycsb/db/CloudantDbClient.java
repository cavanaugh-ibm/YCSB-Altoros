package com.cloudant.ycsb.db;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.lightcouch.DocumentConflictException;
import org.lightcouch.NoDocumentException;

import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.google.common.base.Preconditions;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class CloudantDbClient extends DB {
    // Default configuration
    private static final String DEFAULT_DATABASE_NAME = "usertable";

    // Return codes
    private static final int RC_DOC_NOT_FOUND   = -3;
    private static final int RC_OK              = 0;
    private static final int RC_UPDATE_CONFLICT = -2;

    // Cloudant client and database connector
    private CloudantClient clClient      = null;
    private Database       clDbConnector = null;

    public CloudantDbClient() {
        clClient = null;
        clDbConnector = null;
    }

    @Override
    public void cleanup() throws DBException {
        clDbConnector = null;
        clClient = null;
    }

    @Override
    public int delete(String table, String key) {
        StringToStringMap toDelete = this.executeFind(key);
        if (toDelete == null) {
            return RC_DOC_NOT_FOUND;
        }

        return this.executeRemove(toDelete);
    }

    @Override
    public void init() throws DBException {
        Logger.getLogger("com.cloudant").setLevel(Level.DEBUG);
        Logger.getLogger("org.lightcouch").setLevel(Level.DEBUG);
        Logger.getLogger("org.apache.http").setLevel(Level.DEBUG);

        //
        // Build a Cloudant client based upon the URL, user and password
        String account = getProperties().getProperty("account");
        Preconditions.checkNotNull(account, "Account property may not be null");
        String user = getProperties().getProperty("username");
        Preconditions.checkNotNull(user, "Username property may not be null");
        String pass = getProperties().getProperty("password");
        Preconditions.checkNotNull(pass, "Password property may not be null");
        clClient = new CloudantClient(account, user, pass);

        //
        // Get a handle to the database connector that we will be using
        String dbName = getProperties().getProperty("database", DEFAULT_DATABASE_NAME);
        Preconditions.checkNotNull(dbName, "database property may not be null");
        clDbConnector = clClient.database(dbName, false);
    }

    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        StringToStringMap dataToInsert = new StringToStringMap(values);
        return this.executeInsert(key, dataToInsert);
    }

    @Override
    public int readAll(String table, String key, Map<String, ByteIterator> result) {
        return read(table, key, null, result);
    }

    @Override
    public int readOne(String table, String key, String field, Map<String, ByteIterator> result) {
        return read(table, key, Collections.singleton(field), result);
    }

    @Override
    public int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result) {
        return scan(table, startkey, recordcount, null, result);
    }

    @Override
    public int scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result) {
        return scan(table, startkey, recordcount, Collections.singleton(field), result);
    }

    public int update(String table, String key, Map<String, ByteIterator> values) {
        StringToStringMap queryResult = this.executeFind(key);
        if (queryResult == null) {
            return RC_DOC_NOT_FOUND;
        }
        return this.executeUpdate(updateFields(queryResult, values));
    }

    @Override
    public int updateAll(String table, String key, Map<String, ByteIterator> values) {
        return update(table, key, values);
    }

    @Override
    public int updateOne(String table, String key, String field, ByteIterator value) {
        return update(table, key, Collections.singletonMap(field, value));
    }

    private void copyAllFieldsToResultMap(StringToStringMap inputMap, Map<String, ByteIterator> result) {
        for (String field : inputMap.keySet()) {
            ByteIterator value = inputMap.getAsByteIt(field);
            result.put(field, value);
        }
    }

    private void copyRequestedFieldsToResultMap(Set<String> fields, StringToStringMap inputMap, Map<String, ByteIterator> result) {
        for (String field : fields) {
            ByteIterator value = inputMap.getAsByteIt(field);
            result.put(field, value);
        }
        ByteIterator _id = inputMap.getAsByteIt("_id");
        ByteIterator _rev = inputMap.getAsByteIt("_rev");
        result.put("_id", _id);
        result.put("_rev", _rev);
    }

    private StringToStringMap executeFind(String key) {
        try {
            return clDbConnector.find(StringToStringMap.class, key);
        } catch (NoDocumentException exc) {
            return null;
        }
    }

    private int executeInsert(String key, StringToStringMap dataToWrite) {
        try {
            dataToWrite.put("_id", key);
            clDbConnector.save(dataToWrite);
        } catch (DocumentConflictException exc) {
            return RC_UPDATE_CONFLICT;
        }
        return RC_OK;
    }

    private int executeRemove(StringToStringMap dataToDelete) {
        try {
            clDbConnector.remove(dataToDelete);
        } catch (NoDocumentException exc) {
            return RC_UPDATE_CONFLICT;
        }
        return RC_OK;
    }

    private int executeUpdate(StringToStringMap dataToUpdate) {
        // System.out.println("I = " + dataToUpdate.get("_id"));
        // System.out.println("R = " + dataToUpdate.get("_rev"));
        try {
            clDbConnector.update(dataToUpdate);
        } catch (NoDocumentException exc) {
            return RC_UPDATE_CONFLICT;
        } catch (DocumentConflictException exc) {
            return RC_UPDATE_CONFLICT;
        }

        return RC_OK;
    }

    private Map<String, ByteIterator> getFieldsFromJsonObj(Set<String> fields, JSONObject jsonObj) {
        Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        for (String key : fields) {
            String value = jsonObj.get(key).toString();
            result.put(key, new StringByteIterator(value));
        }
        return result;
    }

    private int read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        StringToStringMap queryResult = this.executeFind(key);
        if (queryResult == null) {
            return RC_DOC_NOT_FOUND;
        }
        if (fields == null) {
            this.copyAllFieldsToResultMap(queryResult, result);
        } else {
            this.copyRequestedFieldsToResultMap(fields, queryResult, result);
        }
        return RC_OK;
    }

    @SuppressWarnings("unchecked")
    private int scan(String table, String startkey, int recordcount, Set<String> fields, List<Map<String, ByteIterator>> result) {
        List<JSONObject> list = clDbConnector.view("_all_docs").startKey(startkey).limit(recordcount).includeDocs(true).query(JSONObject.class);
        for (JSONObject item : list) {
            if (fields == null) {
                result.add(getFieldsFromJsonObj(item.keySet(), item));
            } else {
                result.add(getFieldsFromJsonObj(fields, item));
            }
        }

        return RC_OK;
    }

    private StringToStringMap updateFields(StringToStringMap toUpdate, Map<String, ByteIterator> newValues) {
        for (Entry<String, ByteIterator> entry : newValues.entrySet()) {
            toUpdate.put(entry.getKey(), entry.getValue());
        }

        return toUpdate;
    }
}
