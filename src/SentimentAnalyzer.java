import java.net.URI;
import java.util.Properties;
import java.util.concurrent.*;

import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;

public class SentimentAnalyzer implements org.eclipse.jetty.websocket.api.WebSocketListener {

    private Session session;
    private final StanfordCoreNLP sentimentPipeline;
    private ExecutorService threadPool;
    private final BlockingQueue<String> reviewQueue = new LinkedBlockingQueue<>();
    private static int  analyzedReviewsCount = 0;

    public SentimentAnalyzer(int mode) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        props.setProperty("parse.model", "edu/stanford/nlp/models/lexparser/englishPCFG.caseless.ser.gz");
        props.setProperty("sentiment.model", "edu/stanford/nlp/models/sentiment/sentiment.binary.ser.gz");
        sentimentPipeline = new StanfordCoreNLP(props);

        //Mode 1 is sequential mode and mode 2 is parallel mode
        if (mode == 1) {
            threadPool = Executors.newFixedThreadPool(1);
        } else if (mode == 2) {
            int availableProcessors = Runtime.getRuntime().availableProcessors();
            threadPool = Executors.newFixedThreadPool(availableProcessors);
        } else {
            System.out.println("Choose mode 1 or 2");
        }
    }

    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
    }

    public void onWebSocketText(String message) {
        try {
            reviewQueue.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }

    private int analyzeSentiment(String text) {
        Annotation annotation = new Annotation(text);
        sentimentPipeline.annotate(annotation);

        CoreMap sentence = annotation.get(CoreAnnotations.SentencesAnnotation.class).get(0);
        Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
        analyzedReviewsCount++;
        return RNNCoreAnnotations.getPredictedClass(tree) + 2;
    }

    private String getSentimentLabel(int sentiment) {
        switch (sentiment) {
            case 0:
                return "Very Negative";
            case 1:
                return "Negative";
            case 2:
                return "Neutral";
            case 3:
                return "Positive";
            case 4:
                return "Very Positive";
            default:
                return "Unknown";
        }
    }

    private static Runnable createSentimentAnalysisTask(SentimentAnalyzer clientEndpoint, String review) {
        return () -> {
            try {
                JSONObject mainJson = new JSONObject(review);
                String topic = mainJson.keys().next();
                String reviewJsonString = mainJson.getString(topic);
                JSONObject reviewJson = new JSONObject(reviewJsonString);
                String reviewText = reviewJson.getString("reviewText");
                int sentiment = clientEndpoint.analyzeSentiment(reviewText);
                System.out.println("Analyzed review number: " + analyzedReviewsCount);
//                clientEndpoint.printFormattedResult(topic, reviewJson, reviewText, sentiment);
            } catch (JSONException e) {
                System.out.println("Received non-JSON message: " + review);
                System.out.println();
            }
        };
    }

    private void printFormattedResult(String topic, JSONObject reviewJson, String reviewText, int sentiment) {
        System.out.println("Formatted Message for Topic '" + topic + "':");
        System.out.println("Overall Rating: " + reviewJson.getDouble("overall"));
        System.out.println("Verified: " + reviewJson.getBoolean("verified"));
        System.out.println("Review Time: " + reviewJson.getString("reviewTime"));
        System.out.println("Reviewer ID: " + reviewJson.getString("reviewerID"));
        System.out.println("ASIN: " + reviewJson.getString("asin"));
        System.out.println("Reviewer Name: " + reviewJson.getString("reviewerName"));
        System.out.println("Review Text: " + reviewText);
        System.out.println("Summary: " + reviewJson.getString("summary"));
        System.out.println("Unix Review Time: " + reviewJson.getLong("unixReviewTime"));
        System.out.println("Sentiment: " + getSentimentLabel(sentiment));
        System.out.println();
    }

    private void subscribeToTopic(String topic) {
        sendMessage("topic:" + topic);
    }

    private void subscribeToTopics(List<String> topics) {
        for (String topic : topics) {
            subscribeToTopic(topic);
        }
    }

    @Override
    public void onWebSocketConnect(Session session) {
        System.out.println("Connected to the WebSocket server.");
        this.session = session;
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        System.out.println("Connection closed: " + reason);
        System.out.println("Total Analyzed Reviews: " + analyzedReviewsCount);
        threadPool.shutdown();
        System.exit(0);
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        cause.printStackTrace();
    }

    public void sendMessage(String message) {
        try {
            session.getRemote().sendString(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java DynamicThreadPoolWebSocketClient <durationInMinutes> <mode> <topic1> <topic2> ...");
            System.err.println("Mode: 1 - Sequential, 2 - Parallel");
            System.exit(1);
        }
        long durationInMinutes = Integer.parseInt(args[0]);
        int mode = Integer.parseInt(args[1]);
        List<String> topics = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));

        SentimentAnalyzer clientEndpoint = new SentimentAnalyzer(mode);
        WebSocketClient client = new WebSocketClient();
        long endTime = System.currentTimeMillis() + durationInMinutes * 60 * 1000;

        try {
            client.start();
            URI uri = URI.create("wss://prog3.student.famnit.upr.si/sentiment");
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            Future<Session> future = client.connect(clientEndpoint, uri, request);

            future.get();
            clientEndpoint.subscribeToTopics(topics);
            while (System.currentTimeMillis() < endTime) {
                String review = clientEndpoint.reviewQueue.take();
                Runnable task = createSentimentAnalysisTask(clientEndpoint, review);
                clientEndpoint.threadPool.submit(task);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
