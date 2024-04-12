package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Arrays;

/**
 * Consumes the stream of events.
 * Outputs a stream which handles static file and one stream
 * and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {
    /*
     * Define per task state here. (kv stores etc)
     * READ Samza API part in Writeup to understand how to start
     */
    // userId to user profile
    private KeyValueStore<Integer, Map<String, Object>> userInfo;
    // storeId to business store profile
    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    // Tags for businesses categories
    private Set<String> lowCalories;

    private Set<String> energyProviders;

    private Set<String> willingTour;

    private Set<String> stressRelease;

    private Set<String> happyChoice;

    private Integer durationThreshold = 300000;

    private void initSets() {
        lowCalories = new HashSet<>(Arrays.asList("seafood", "vegetarian", "vegan", "sushi"));
        energyProviders = new HashSet<>(Arrays.asList("bakeries", "ramen", "donuts", "burgers",
                "bagels", "pizza", "sandwiches", "icecream",
                "desserts", "bbq", "dimsum", "steak"));
        willingTour = new HashSet<>(Arrays.asList("parks", "museums", "newamerican", "landmarks"));
        stressRelease = new HashSet<>(Arrays.asList("coffee", "bars", "wine_bars", "cocktailbars", "lounges"));
        happyChoice = new HashSet<>(Arrays.asList("italian", "thai", "cuban", "japanese", "mideastern",
                "cajun", "tapas", "breakfast_brunch", "korean", "mediterranean",
                "vietnamese", "indpak", "southern", "latin", "greek", "mexican",
                "asianfusion", "spanish", "chinese"));
    }

    // tag related helper
    private String getStoreTag(String cate) {
        String tag = "";
        if (happyChoice.contains(cate)) {
            tag = "happyChoice";
        } else if (stressRelease.contains(cate)) {
            tag = "stressRelease";
        } else if (willingTour.contains(cate)) {
            tag = "willingTour";
        } else if (energyProviders.contains(cate)) {
            tag = "energyProviders";
        } else if (lowCalories.contains(cate)) {
            tag = "lowCalories";
        } else {
            tag = "others";
        }
        return tag;
    }

    private Set<String> getUserTags(Map<String, Object> userProfile) {
        Integer mood = (Integer) userProfile.get("mood");
        Integer stress = (Integer) userProfile.get("stress");
        Integer bloodSugar = (Integer) userProfile.get("blood_sugar");
        Integer active = (Integer) userProfile.get("active");

        if ((Integer) userProfile.get("userId") == 3) {
            System.out.println("mood:       " + mood.toString());
            System.out.println("bloodSugar: " + bloodSugar.toString());
            System.out.println("stress:     " + stress.toString());
            System.out.println("active:     " + active.toString());
        }

        Set<String> tags = new HashSet<String>();
        if (bloodSugar.intValue() > 4 && mood.intValue() > 6 && active.intValue() == 3) {
            tags.add("lowCalories");
        }
        if (bloodSugar.intValue() < 2 || mood.intValue() < 4) {
            /// DEBUG
            if ((Integer) userProfile.get("userId") == 3) {
                System.out.println("after riderStatus update, client 3 should not have energyProviders tag");
                System.out.println(bloodSugar.intValue());
                System.out.println(mood.intValue());
            }
            tags.add("energyProviders");
        }
        if (active.intValue() == 3) {
            tags.add("willingTour");
        }
        if (stress.intValue() > 5 || active.intValue() == 1 || mood.intValue() < 4) {
            tags.add("stressRelease");
        }
        if (mood.intValue() > 6) {
            tags.add("happyChoice");
        }
        if (tags.size() == 0) {
            // if rider has not been matched to any of the above tag
            tags.add("others");
        }
        return tags;
    }

    private Boolean tagMatch(String storeTag, Set<String> userTags) {
        return userTags.contains(storeTag);
    }

    // match score helper
    private Integer getDeviceValue(String device) {
        if (device.equals("iPhone 5")) {
            return 1;
        } else if (device.equals("iPhone 7")) {
            return 2;
        } else if (device.equals("iPhone XS")) {
            return 3;
        } else {
            return 0;
        }
    }

    private Integer getPriceValue(String price) {
        if (price.equals("$")) {
            return 1;
        } else if (price.equals("$$")) {
            return 2;
        } else if (price.equals("$$$") || price.equals("$$$$")) { // $$$ or $$$$
            return 3;
        } else {
            return 0;
        }
    }

    private Double getDistance(Double lon1, Double lat1, Double lon2, Double lat2, String unit) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0.0;
        } else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            if (unit.equals("K")) {
                dist = dist * 1.609344;
            } else if (unit.equals("N")) {
                dist = dist * 0.8684;
            }
            return (dist);
        }
    }

    private Double getMatchScore(Map<String, Object> userProfile, Map<String, Object> storeProfile,
                Double userLongitude, Double userLatitude) {
        // store info
        System.out.println("getMatchScore called on " + userProfile.get("userId").toString() + " and " + storeProfile.get("name").toString());
        Integer reviewCount = (Integer) storeProfile.get("review_count");
        Double rating = (Double) storeProfile.get("rating");
        String category = (String) storeProfile.get("categories");
        String price = (String) storeProfile.get("price");
        Double storeLongitude = (Double) storeProfile.get("longitude");
        Double storeLatitude = (Double) storeProfile.get("latitude");
        // user info
        Integer age = (Integer) userProfile.get("age");
        String interest = (String) userProfile.get("interest");
        Integer travelCount = (Integer) userProfile.get("travel_count");
        String device = (String) userProfile.get("device");

        // initial match score
        Double score = reviewCount * rating;
        System.out.println("    base score = " + reviewCount.toString() + " * " + rating.toString() + " = " + score.toString());
        // interest exact match +10
        if (category.equals(interest)) {
            System.out.println("    bonus added! store category matches user interest: " + interest.toString());
            score += 10.0;
        }
        // device and price match score
        Integer valueMatchScore = Math.abs(getPriceValue(price) - getDeviceValue(device));
        if (valueMatchScore > 0) {
            System.out.println("    device & price match penalty: valueMatchScore = " + valueMatchScore.toString());
            score *= (1.0 - 0.1 * valueMatchScore);
        }
        // distance score
        Double distance = getDistance(userLongitude, userLatitude, storeLongitude, storeLatitude, "M");

        Double distanceThres;
        if (age.intValue() > 20 && travelCount.intValue() <= 50) {
            distanceThres = 5.0; // unit: mile
        } else {
            distanceThres = 10.0; // unit: mile
        }

        System.out.println("    distance threshold for " + userProfile.get("userId").toString() + " is " + distanceThres.toString());

        if (distance > distanceThres) {
            System.out.println("    distance penalty added, distance = " + distance.toString());
            score *= 0.1;
        }
        System.out.println("    final score = " + score.toString());
        return score;
    }

    // boilerplate code for init keyvaluestore
    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize kv store

        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getTaskContext().getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("yelp-info");

        // Initialize store tags set
        initSets();

        // Initialize static data and save them in kv store
        initialize("UserInfoData.json", "NYCstore.json");
    }

    /**
     * This function will read the static data from resources folder
     * and save data in KV store.
     * <p>
     * This is just an example, feel free to change them.
     */
    public void initialize(String userInfoFile, String businessFile) {
        List<String> userInfoRawString = AdMatchConfig.readFile(userInfoFile);
        System.out.println("Reading user info file from " + Resources.getResource(userInfoFile).toString());
        System.out.println("UserInfo raw string size: " + userInfoRawString.size());
        for (String rawString : userInfoRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                int userId = (Integer) mapResult.get("userId");
                userInfo.put(userId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse user info :" + rawString);
            }
        }

        List<String> businessRawString = AdMatchConfig.readFile(businessFile);

        System.out.println("Reading store info file from " + Resources.getResource(businessFile).toString());
        System.out.println("Store raw string size: " + businessRawString.size());

        for (String rawString : businessRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                String storeId = (String) mapResult.get("storeId");
                String cate = (String) mapResult.get("categories");
                String tag = getStoreTag(cate);
                mapResult.put("tag", tag);
                yelpInfo.put(storeId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse store info :" + rawString);
            }
        }
    }

    public void processRiderInterest(Integer userId, String interest, Integer duration) {
        // TODO: fill in
        if (duration > durationThreshold) {
            Map<String, Object> userProfile = userInfo.get(userId);
            userProfile.put("interest", interest);
        }
    }

    public void processRiderStatus(Integer userId, Integer mood, Integer bloodSugar,
            Integer stress, Integer active) {
        // TODO: fill in
        System.out.println("processRiderStatus(" + userId.toString() + ", " + mood.toString() + ", " + bloodSugar.toString() + ", " + stress.toString() + ", " + active.toString() + ")...");
        Map<String, Object> userProfile = userInfo.get(userId);
        userProfile.put("mood", mood);
        userProfile.put("blood_sugar", bloodSugar);
        userProfile.put("stress", stress);
        userProfile.put("active", active);
        System.out.println("after processRiderStatus, userId " + userId.toString() + " becomes...");
        System.out.println(userInfo.get(userId).toString());
    }

    public void processRideRequest(String blockId, Integer clientId, Double userLongitude,
            Double userLatitude, String genderPreference, MessageCollector collector) {
        // TODO: fill in
        System.out.println("processRideRequest(" + clientId.toString() + ")...");
        Map<String, Object> userProfile = userInfo.get(clientId);
        System.out.println(userProfile.toString());
        Set<String> userTags = getUserTags(userProfile);

        String bestStoreId = null;
        Double bestStoreScore = -1.0;

        KeyValueIterator<String, Map<String, Object>> iter = yelpInfo.all();
        try {
            while (iter.hasNext()) {
                Entry<String, Map<String, Object>> entry = iter.next();
                String storeId = entry.getKey();
                Map<String, Object> storeProfile = entry.getValue();
                String storeTag = (String) storeProfile.get("tag");

                System.out.println(storeProfile.get("name").toString() + " has tag: " + storeTag + ", userId " + clientId + " has tags: " + userTags.toString());
                if (!tagMatch(storeTag, userTags)) {
                    // only process stores that have matching tags
                    continue;
                }

                Double storeScore = getMatchScore(userProfile, storeProfile, userLongitude, userLatitude);
                if (storeScore > bestStoreScore) {
                    bestStoreId = storeId;
                    bestStoreScore = storeScore;
                }
            }
        } finally {
            iter.close();
        }

        // output best business match with client
        if (bestStoreId != null) {
            Map<String, Object> bestStoreProfile = yelpInfo.get(bestStoreId);
            String bestStoreName = (String) bestStoreProfile.get("name");

            Map<String, Object> outputMessage = new HashMap<>();
            outputMessage.put("userId", clientId);
            outputMessage.put("storeId", bestStoreId);
            outputMessage.put("name", bestStoreName);

            OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(
                    AdMatchConfig.AD_STREAM, outputMessage);
            collector.send(envelope);
        }
    }

    public void processEvents(Map<String, Object> msg, MessageCollector collector) {
        String type = (String) msg.get("type");
        if (type.equals("RIDE_REQUEST")) {
            String blockId = msg.get("blockId").toString();
            Integer clientId = (Integer) msg.get("clientId");
            Double longitude = (Double) msg.get("longitude");
            Double latitude = (Double) msg.get("latitude");
            String genderPreference = (String) msg.get("gender_preference");
            processRideRequest(blockId, clientId, longitude, latitude, genderPreference, collector);
        } else if (type.equals("RIDER_INTEREST")) {
            Integer userId = (Integer) msg.get("userId");
            String interest = msg.get("interest").toString();
            Integer duration = (Integer) msg.get("duration");
            processRiderInterest(userId, interest, duration);
        } else if (type.equals("RIDER_STATUS")) {
            Integer userId = (Integer) msg.get("userId");
            Integer mood = (Integer) msg.get("mood");
            Integer bloodSugar = (Integer) msg.get("blood_sugar");
            Integer stress = (Integer) msg.get("stress");
            Integer active = (Integer) msg.get("active");
            processRiderStatus(userId, mood, bloodSugar, stress, active);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector,
            TaskCoordinator coordinator) {
        /*
         * All the messsages are partitioned by blockId, which means the messages
         * sharing the same blockId will arrive at the same task, similar to the
         * approach that MapReduce sends all the key value pairs with the same key
         * into the same reducer.
         */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            processEvents((Map<String, Object>) envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException(
                    "Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
}
