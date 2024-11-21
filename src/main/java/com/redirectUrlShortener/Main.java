package com.redirectUrlShortener;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        String pathParameter = input.get("rawPath").toString();
        String shortUrlCode = pathParameter.replace("/", "");

        if (shortUrlCode.isEmpty()) {
            throw new IllegalArgumentException("Invalid input: 'shortUrlCode' is required.");
        }

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("url-shortener-java-storage")
                .key(shortUrlCode + ".json")
                .build();

        InputStream s3ObjectStream;

        try {
            s3ObjectStream = s3Client.getObject(getObjectRequest);
        } catch (Exception e) {
            throw new RuntimeException("Error fetching URL data from S3: " + e.getMessage(), e);
        }

        UrlData urlData;

        try {
            urlData = objectMapper.readValue(s3ObjectStream, UrlData.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing URL data: " + exception.getMessage(), e);
        }

        long currentTimeInSeconds = System.currentTimeMillis() / 1000;

        Map<String, Object> response = new HashMap<>();

        if (urlData.getExpirationTime() < currentTimeInSeconds) {
            response.put("statusCode", 410);
            response.put("body", "URL has expired");
            return response;
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("Location", urlData.getOriginalUrl());
        response.put("headers", headers);
        response.put("statusCode", 302);

        return response;
    }
}