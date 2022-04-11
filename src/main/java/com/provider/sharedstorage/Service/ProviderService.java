package com.provider.sharedstorage.Service;

import com.amazonaws.auth.BasicSessionCredentials;
import com.provider.sharedstorage.Repository.AWSRepository;
import com.provider.sharedstorage.Repository.ProviderRepository;
import com.provider.sharedstorage.Repository.SQLRepository;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

@Service
public class ProviderService {

    String RoleARN = "arn:aws:iam::844102931058:role/test_AssumeRole_VCM";
    private final ProviderRepository providerRepository;
    private  final SQLRepository sqlRepository;
    private final AWSRepository awsRepository;

    private static BasicSessionCredentials awsCredentials;

    public ProviderService(ProviderRepository providerRepository,
                           SQLRepository sqlRepository,
                           AWSRepository awsRepository) {

        this.providerRepository = providerRepository;
        this.sqlRepository = sqlRepository;
        this.awsRepository = awsRepository;

        awsCredentials = this.awsRepository.assumeRole(RoleARN);
    }

    public Object createShare(String requestBody) {
        JSONObject share;
        JSONObject output = new JSONObject();
        try {
            JSONObject body = (JSONObject) new JSONParser().parse(requestBody);

            System.out.println(awsCredentials.getAWSAccessKeyId());
            System.out.println(awsCredentials.getAWSSecretKey());
            JSONObject datasetOutput = awsRepository.createDataset(awsCredentials, (JSONArray) body.get("datasets"));
            if (datasetOutput.get("statusCode").toString().equals("500")) {
                return datasetOutput;
            }
            share = providerRepository.createShare(body.get("name").toString(),
                    body.get("description").toString(),
                    body.get("model").toString(),
                    body.get("direction").toString());
            if ( share.get("statusCode").equals(500)) return share;
            sqlRepository.insertDatasets(share.get("shareId").toString(), (JSONArray) body.get("datasets"));
            sqlRepository.insertSubscribers(share.get("shareId").toString(), body.get("subscribers"));

            return share;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public JSONArray getShares(String shareId) {
        JSONArray shares = sqlRepository.getShares(shareId);
        return shares;
    }

    public Object updateShare(String shareId, String requestBody) {
        String sql = "";
        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject body =  (JSONObject) jsonParser.parse(requestBody);
            sql = "Update shares SET ";
            for (String s: body.keySet()) {
                sql += s + "='" + body.get(s).toString() + "',";
            }
            sql = sql.substring(0, sql.length()-1) +  " where id = '"+ shareId + "'";
            System.out.println(sql);
            sqlRepository.insertData(sql);
            return this.getShares(shareId);
        } catch ( ParseException e) {
            e.printStackTrace();
        }
        return "";
    }

    public Object addDatasets(String shareId, String datasets) {
        try {
            JSONObject dataset = (JSONObject) new JSONParser().parse(datasets);
            JSONArray array = new JSONArray();
            array.add(dataset);
            JSONObject createDataset = awsRepository.createDataset(awsCredentials, array);
            if (createDataset.get("statusCode").equals("500")) {
                return createDataset;
            }
            sqlRepository.insertDatasets(shareId, array);
            return createDataset;
        } catch ( Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public JSONArray getDatasets(String shareId, String datasetId) {
        String sql = "Select * from datasets where share_id = '"+shareId+"'";
        if ( datasetId != null) {
            sql = sql + "and id = '" +datasetId+ "'";
        }
        return sqlRepository.getData(sql);
    }

    public Object addSubscriber(String shareId, String subscriber) {
        return providerRepository.addSubscribers(subscriber, shareId);
    }

    public Object deleteDataset(String shareId, String datasetId) {
        JSONArray datasets = this.getDatasets(shareId, datasetId);
        JSONObject dataset = (JSONObject) datasets.get(0);
       // awsRepository.deleteBucket(dataset.getAsString("location"), awsCredentials);
        return sqlRepository.deleteDataset(shareId, datasetId);

    }

    public Object deleteSubscriber(String shareId, String subscriberName) {
        return sqlRepository.deleteSubscriber(shareId, subscriberName);
    }

    public Object addAssetsToDataset(String requestBody, String shareId, String datasetId) {
        JSONArray datasets = this.getDatasets(shareId, datasetId);
        JSONObject dataset = (JSONObject) datasets.get(0);
        try {
            JSONObject body = (JSONObject) new JSONParser().parse(requestBody);
            return awsRepository.addAssets(awsCredentials, dataset.getAsString("location"), body.getAsString("fileName"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Object downloadAssets(String shareId, String datasetId) {
        JSONArray datasets = this.getDatasets(shareId, datasetId);
        JSONObject dataset = (JSONObject) datasets.get(0);
        return awsRepository.downloadAssets(awsCredentials, dataset.getAsString("location"));
    }

    public Object getAssets(String shareId, String datasetId) {
        JSONArray datasets = this.getDatasets(shareId, datasetId);
        JSONObject dataset = (JSONObject) datasets.get(0);
        return awsRepository.getAssets(awsCredentials, dataset.getAsString("location"));

    }


    public JSONObject updateDataset(String shareId, String datasetId, String body) {
        JSONObject output = new JSONObject();
        try {

            JSONObject dataset = (JSONObject) new JSONParser().parse(body);

            if ( dataset.size() == 0) {
                output.put("Error", "Empty Body");
                output.put("statusCode", 400);
                return output;
            }

            if ( dataset.getAsString("location") != null) {
                JSONArray datasets = new JSONArray();
                datasets.add(dataset);
                JSONObject datasetOutput = awsRepository.createDataset(awsCredentials, datasets);
                if (datasetOutput.get("statusCode").toString().equals("500")) {
                    return datasetOutput;
                }
            }
            String sql = "Update datasets SET ";
            for (String s: dataset.keySet()) {
                sql += s + "='" + dataset.get(s).toString() + "',";
            }
            sql = sql.substring(0, sql.length()-1) +  " where share_id = '"+ shareId + "' and id = '"+ datasetId + "'";
            sqlRepository.insertData(sql);
            output.put("Message", "Dataset has been updated");
            output.put("Dataset", this.getDatasets(shareId, datasetId));
            return output;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Object getSubscribers(String shareId) {
        String sql = "select share_id, name, email from subscribers where share_id = '" + shareId +"'";
        return sqlRepository.getData(sql);
    }

    public Object synAssumeRole(String shareId, String datasetId) {
        awsCredentials = awsRepository.assumeRole(RoleARN);
        JSONObject jsonObject = new JSONObject();

        jsonObject.put("accessKey", awsCredentials.getAWSAccessKeyId());
        jsonObject.put("secretKey", awsCredentials.getAWSSecretKey());
        return jsonObject;

    }
}
