import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import mpi.*;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

public class SentimentAnalyzerMPI implements org.eclipse.jetty.websocket.api.WebSocketListener{
    private Session session;
    private final BlockingQueue<String> reviewQueue = new LinkedBlockingQueue<>();
    private static final int TERMINATION_TAG = 999;
    private static final int LENGTH_TAG = 1;
    private static final int SENTIMENT_TAG = 2;
    public SentimentAnalyzerMPI() {
    }

    public void onWebSocketText(String message) {
        try {
            String review = getReviewFromJson(message);
            if(!Objects.equals(review, "Non-JSON")){
                reviewQueue.put(review);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }
    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
    }
    @Override
    public void onWebSocketConnect(Session session) {
        System.out.println("Connected to the WebSocket server.");
        this.session = session;
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        System.out.println("Connection closed: " + reason);
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

    private static int analyzeSentiment(String text) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        props.setProperty("parse.model", "edu/stanford/nlp/models/lexparser/englishPCFG.caseless.ser.gz");
        props.setProperty("sentiment.model", "edu/stanford/nlp/models/sentiment/sentiment.binary.ser.gz");
        StanfordCoreNLP sentimentPipeline = new StanfordCoreNLP(props);

        Annotation annotation = new Annotation(text);
        sentimentPipeline.annotate(annotation);

        CoreMap sentence = annotation.get(CoreAnnotations.SentencesAnnotation.class).get(0);
        Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
        return RNNCoreAnnotations.getPredictedClass(tree);
    }

    private String getReviewFromJson( String review) {
            try {
                JSONObject mainJson = new JSONObject(review);
                String topic = mainJson.keys().next();
                String reviewJsonString = mainJson.getString(topic);
                JSONObject reviewJson = new JSONObject(reviewJsonString);
                return reviewJson.getString("reviewText");
            } catch (JSONException e) {
                System.out.println("Received non-JSON message: " + review);
                return "Non-JSON";
            }
    }
    private void subscribeToTopic(String topic) {
        sendMessage("topic:" + topic);
    }

    private void subscribeToTopics(List<String> topics) {
        for (String topic : topics) {
            subscribeToTopic(topic);
        }
    }

    public static void main(String[] args) throws MPIException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        int durationInMinutes = Integer.parseInt(args[3]);
        List<String> topics = Arrays.asList(Arrays.copyOfRange(args, 4, args.length));

        if (size < 2) {
            System.err.println("This program must be run with at least two processes.");
            MPI.Finalize();
            System.exit(1);
        }

        if (rank == 0) {
            // Master process
            MPICoordinator(durationInMinutes, topics);
        } else {
            // Worker process
            MPIWorker(rank);
        }

        MPI.Finalize();
        System.exit(0);
    }

    private static void MPICoordinator(int duration, List<String> topics) {
        SentimentAnalyzerMPI clientEndpoint = new SentimentAnalyzerMPI();
        WebSocketClient client = new WebSocketClient();
        long endTime = System.currentTimeMillis() + duration * 60 * 1000;

        int receivedTasksCount = 0;
        try {
            client.start();
            URI uri = URI.create("wss://prog3.student.famnit.upr.si/sentiment");
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            Future<Session> future = client.connect(clientEndpoint, uri, request);
            future.get();
            clientEndpoint.subscribeToTopics(topics);

            List<Integer> sentimentResults = new ArrayList<>();

            for (int i = 1; i < MPI.COMM_WORLD.Size(); i++) {
                String nextReview = clientEndpoint.reviewQueue.take();
                int reviewLength = nextReview.length();
                MPI.COMM_WORLD.Send(new int[]{reviewLength}, 0, 1, MPI.INT, i, LENGTH_TAG);
                MPI.COMM_WORLD.Send(nextReview.toCharArray(), 0, reviewLength, MPI.CHAR, i, SENTIMENT_TAG);
            }

            while (System.currentTimeMillis() < endTime) {
                if (!clientEndpoint.reviewQueue.isEmpty()) {
                    int[] sentiment = new int[1];
                    Status status = MPI.COMM_WORLD.Recv(sentiment, 0, 1, MPI.INT, MPI.ANY_SOURCE, SENTIMENT_TAG);
                    sentimentResults.add(sentiment[0]);
                    receivedTasksCount++;
                    int who = status.source;
                    String nextTask = clientEndpoint.reviewQueue.take();
                    int taskLength = nextTask.length();
                    MPI.COMM_WORLD.Send(new int[]{taskLength}, 0, 1, MPI.INT, who, LENGTH_TAG);
                    MPI.COMM_WORLD.Send(nextTask.toCharArray(), 0, taskLength, MPI.CHAR, who, SENTIMENT_TAG);
                }
            }

            for (int i = 1; i < MPI.COMM_WORLD.Size(); i++) {
                int[] sentiment = new int[1];
                Status status = MPI.COMM_WORLD.Recv(sentiment, 0, 1, MPI.INT, MPI.ANY_SOURCE, SENTIMENT_TAG);
                int who = status.source;
                MPI.COMM_WORLD.Send(new int[0], 0, 0, MPI.INT, who, TERMINATION_TAG);
                System.out.println("Termination sent to " + who);
            }
            System.out.println("-----------------------------------------------");
            System.out.println("Total Analyzed Reviews: " + receivedTasksCount);
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

    private static void MPIWorker(int rank) throws MPIException {
        char[] task = new char[5000];
        int[] taskLength = new int[1];
        try {
            while (true) {
                Status status = MPI.COMM_WORLD.Recv(taskLength, 0, 1, MPI.INT, 0, MPI.ANY_TAG);
                if (status.tag == TERMINATION_TAG) {
                    System.out.println("Worker " + rank + " done");
                    break;
                }
                Status status2 = MPI.COMM_WORLD.Recv(task, 0, taskLength[0], MPI.CHAR, 0, MPI.ANY_TAG);
                if (status2.tag == TERMINATION_TAG) {
                    System.out.println("Worker " + rank + " done");
                    break;
                }
                String review = new String(task, 0, taskLength[0]);
                int[] sentiment = new int[1];
                sentiment[0] = analyzeSentiment(review);
                MPI.COMM_WORLD.Send(sentiment, 0, 1, MPI.INT, 0, SENTIMENT_TAG);
                System.out.println("Worker " + rank + " did a task");
            }
        } catch (MPIException e) {
            e.printStackTrace();
        }
    }
}
