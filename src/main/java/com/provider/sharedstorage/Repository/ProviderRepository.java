package com.provider.sharedstorage.Repository;

import com.amazonaws.auth.BasicSessionCredentials;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.springframework.stereotype.Repository;

import java.util.Objects;
import java.util.UUID;

@Repository
public class ProviderRepository {

    String RoleARN = "arn:aws:iam::844102931058:role/test_AssumeRole_VCM";
    private final AWSRepository awsRepository;
    private final SQLRepository sqlRepository;
    private static BasicSessionCredentials awsCredentials;

    public ProviderRepository(AWSRepository awsRepository,
                              SQLRepository sqlRepository) {

        this.awsRepository = awsRepository;
        this.sqlRepository = sqlRepository;
    }

    public JSONObject createShare(String name, String description, String model, String direction) {
        UUID shareId = UUID.randomUUID();
        JSONObject share = new JSONObject();
        share.put("name", name);
        share.put("description", description);
        share.put("model", model);
        share.put("direction", direction);
        share.put("shareId", shareId);

        return sqlRepository.createShare(share);

    }

    public Object addDatasets(String body, String shareId) {
        JSONObject output = new JSONObject();

        JSONArray datasets = new JSONArray();
        try {
            Object obj = new JSONParser().parse(body);
            if (obj.getClass().getSimpleName().equalsIgnoreCase("JSONArray")) {
                datasets = (JSONArray) obj;
            } else {
                JSONObject dataset = new JSONObject();
                datasets.add(dataset);
            }
            sqlRepository.insertDatasets(shareId, datasets);
            output.put("Message", "Created the dataset successfully");
            output.put("statusCode", 200);

        } catch (ParseException e) {
            e.printStackTrace();
            output.put("Message", "Failed in creating the dataset");
            output.put("Error", e.getMessage());
            output.put("statusCode", 500);

        }
        return output;
    }

    public Object addSubscribers(String subscribers, String shareId) {
        JSONObject output = new JSONObject();

        try {
            JSONObject subscriber = (JSONObject) new JSONParser().parse(subscribers);
            sqlRepository.insertSubscribers(shareId, subscriber);
            output.put("statusCode", 200);
            output.put("Message", "subscriber has been added");
        } catch (ParseException e) {
            e.printStackTrace();
            output.put("statusCode", 500);
            output.put("Message", "Failed to add the subscriber");
        }
        return output;
    }



}
