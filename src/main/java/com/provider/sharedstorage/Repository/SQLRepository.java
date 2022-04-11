package com.provider.sharedstorage.Repository;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.UUID;

@Repository
public class SQLRepository {
    private final String url = "jdbc:mysql://localhost:3306/jdbc-share";
    private final String user = "root";
    private final String pass = "Raghav@123#";

    @Autowired
    public SQLRepository () {}

    public Statement getStatement(String url, String user, String password) {
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            System.out.println(statement);
            return statement;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void insertData(String sql) {
        try {
            Statement statement = this.getStatement(this.url, this.user, this.pass);
            statement.executeUpdate(sql);
        } catch( SQLException e) {
            e.printStackTrace();
        }
    }

    public JSONArray getData(String sql) {
        try {
            Statement statement = this.getStatement(this.url, this.user, this.pass);
            ResultSet resultSet = statement.executeQuery(sql);

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnsNumber = resultSetMetaData.getColumnCount();

            JSONArray output = new JSONArray();
            while (resultSet.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i <= columnsNumber; i++) {
                    if (resultSet.getString(i) != null)
                        jsonObject.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
                }
                output.add(jsonObject);
            }
            return output;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new JSONArray();
    }

    public JSONArray getShares(String shareId) {
        String sqlText = "Select * from shares where model = 'shared-storage' and direction = 'outbound'";
        if ( shareId != null) {
            sqlText = sqlText + " and id = '" +shareId+ "'";
        }
        try {
            JSONArray result = this.getData(sqlText);
            return result;
        } catch ( Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void insertDatasets(String shareId, JSONArray datasets) {

        String sqlText;
        try {
            for (Object o : datasets) {
                UUID datasetId = UUID.randomUUID();
                JSONObject dataset = (JSONObject) o;
                sqlText = "INSERT INTO datasets (id, share_id, dataset_name, description, location) Values (" +
                        "'" + datasetId + "'," +
                        "'" + shareId + "'," +
                        "'" + dataset.get("name").toString() + "'," +
                        "'" + dataset.get("description").toString() + "'," +
                        "'" + dataset.get("location").toString() + "')";

                System.out.println(sqlText);

                Statement statement = this.getStatement(this.url, this.user, this.pass);
                statement.executeUpdate(sqlText);
            }
        } catch (SQLException e ) {
            e.printStackTrace();
        }

    }
    public JSONObject createShare(JSONObject body) {

        String sqlText = "INSERT INTO shares (id, name, direction, description, model) Values (" +
                "'" +body.get("shareId").toString()+ "'," +
                "'" +body.get("name").toString() + "'," +
                "'" +body.get("direction").toString() + "'," +
                "'" +body.get("description").toString() + "'," +
                "'" +body.get("model").toString() + "')";

        JSONObject output = new JSONObject();
        output.put("shareId", body.get("shareId").toString());

        try {
            this.insertData(sqlText);
            output.put("message", "Share Created");
            output.put("statusCode", 200);
        } catch (Exception e) {
            e.printStackTrace();
            output.put("message", "failed to create the Share");
            output.put("statusCode", 500);
        }

        return output;

    }

    public void insertSubscribers(String shareId, Object subscriber) {

        JSONArray subscribers = new JSONArray();
        if (subscriber.getClass().getSimpleName().equals("JSONObject")) {
            subscribers.add(subscriber);
        } else {
            subscribers = (JSONArray) subscriber;
        }
        String model = "shared-storage";

        if (subscribers.isEmpty()) return;
        String sqlText;
        try {
            for (Object o : subscribers) {
                JSONObject s = (JSONObject) o;

                sqlText = "INSERT INTO subscribers (share_id, name, email, model) Values (" +
                        "'" + shareId + "'," +
                        "'" + s.get("name").toString() + "'," +
                        "'" + s.get("email").toString() + "'," +
                        "'" + model + "')";

                Statement statement = this.getStatement(this.url, this.user, this.pass);
                statement.executeUpdate(sqlText);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public String deleteSubscriber(String shareId, String name) {
        String sqlText = "Delete from subscribers where share_id = '" + shareId + "'" + " and name = '" + name + "'";
        try {
            Statement statement = this.getStatement(this.url, this.user, this.pass);
            statement.executeUpdate(sqlText);
            return "subscriber deleted successfully";
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return "Failed to delete the subscriber";
    }

    public JSONObject deleteDataset(String shareId, String datasetId) {
        String sql = "Delete from datasets where share_id = '" +shareId+ "' and id = '" +datasetId+ "'";
        Statement statement = this.getStatement(this.url, this.user, this.pass);
        JSONObject output = new JSONObject();
        try {
            statement.executeUpdate(sql);
            output.put("Message", "Dataset has been deleted");
            output.put("statusCode", 200);
        } catch ( SQLException e) {
            e.printStackTrace();
            output.put("Error", "Failed to delete the dataset");
            output.put("statusCode", 500);
        }
        return output;
    }
}
