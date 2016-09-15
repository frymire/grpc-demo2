package io.grpc.examples.routeguide;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.routeguide.RouteGuideGrpc.RouteGuideBlockingStub;
import io.grpc.examples.routeguide.RouteGuideGrpc.RouteGuideStub;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

 // Sample client code that makes gRPC calls to the server.
public class RouteGuideClient {
  
  private final ManagedChannel channel;
  private final RouteGuideBlockingStub blockingStub;
  private final RouteGuideStub asyncStub;

  private final Logger logger = Logger.getLogger(RouteGuideClient.class.getName());

  // Construct client for accessing RouteGuide server using the existing channel.
  public RouteGuideClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = RouteGuideGrpc.newBlockingStub(channel);
    asyncStub = RouteGuideGrpc.newStub(channel);
  }

  // Construct client for accessing RouteGuide server at host:port.
  public RouteGuideClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  // Blocking unary call example.  Calls getFeature and prints the response.
  public void getFeature(int lat, int lon) {
    
    info("*** GetFeature: lat={0} lon={1}", lat, lon);

    Point request = Point.newBuilder().setLatitude(lat).setLongitude(lon).build();

    Feature feature;
    try {
      feature = blockingStub.getFeature(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    
    if (RouteGuideUtil.exists(feature)) {
      info("Found feature called \"{0}\" at {1}, {2}",
          feature.getName(),
          RouteGuideUtil.getLatitude(feature.getLocation()),
          RouteGuideUtil.getLongitude(feature.getLocation()));
    } else {
      info("Found no feature at {0}, {1}",
          RouteGuideUtil.getLatitude(feature.getLocation()),
          RouteGuideUtil.getLongitude(feature.getLocation()));
    }
    
  }

  // Blocking server-streaming example. Calls listFeatures with a rectangle of interest. 
  // Prints each response feature as it arrives.
  public void listFeatures(int lowLat, int lowLon, int hiLat, int hiLon) {
    
    info("Get features in a rectangle...\nlowLat={0} lowLon={1} hiLat={2} hiLon={3}", lowLat, lowLon, hiLat, hiLon);

    Point lowPoint = Point.newBuilder().setLatitude(lowLat).setLongitude(lowLon).build();
    Point highPoint = Point.newBuilder().setLatitude(hiLat).setLongitude(hiLon).build();
    Rectangle request = Rectangle.newBuilder().setLo(lowPoint).setHi(highPoint).build();
    
    Iterator<Feature> features;
    try {
      features = blockingStub.listFeatures(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }

    StringBuilder responseLog = new StringBuilder("Result: ");
    while (features.hasNext()) {
      Feature feature = features.next();
      responseLog.append(feature);
    }
    info(responseLog.toString());
  }

  // Async client-streaming example. Sends numPoints randomly chosen points from features with 
  // a variable delay in between. Prints the statistics when they are sent from the server.
  public void recordRoute(List<Feature> features, int numPoints) throws InterruptedException {
    
    info("Record a route...");
    
    final CountDownLatch finishLatch = new CountDownLatch(1);
    
    StreamObserver<RouteSummary> responseObserver = new StreamObserver<RouteSummary>() {
      
      @Override public void onNext(RouteSummary summary) {
        info("Finished trip with {0} points. Passed {1} features. Travelled {2} meters. It took {3} seconds.", 
            summary.getPointCount(), summary.getFeatureCount(), summary.getDistance(), summary.getElapsedTime());
      }

      @Override public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        logger.log(Level.WARNING, "RecordRoute Failed: {0}", status);
        finishLatch.countDown();
      }

      @Override public void onCompleted() {
        info("Finished RecordRoute");
        finishLatch.countDown();
      }
    };

    StreamObserver<Point> requestObserver = asyncStub.recordRoute(responseObserver);
    try {
      
      // Send numPoints points randomly selected from the features list.
      Random rand = new Random();
      for (int i = 0; i < numPoints; ++i) {
        int index = rand.nextInt(features.size());
        Point point = features.get(index).getLocation();
        info("Visiting point {0}, {1}", RouteGuideUtil.getLatitude(point), RouteGuideUtil.getLongitude(point));
        requestObserver.onNext(point);
        
        // Sleep for a bit before sending the next one.
        Thread.sleep(rand.nextInt(1000) + 500);
        if (finishLatch.getCount() == 0) {
          // RPC completed or errored before we finished sending.
          // Sending further requests won't error, but they will just be thrown away.
          return;
        }
        
      }
      
    } catch (RuntimeException e) {
      requestObserver.onError(e); // Cancel RPC
      throw e;
    }
    // Mark the end of requests
    requestObserver.onCompleted();

    // Receiving happens asynchronously
    finishLatch.await(1, TimeUnit.MINUTES);
  }

  // Bi-directional example, which can only be asynchronous. 
  // Send some chat messages, and print any messages received from the server.
  public void routeChat() throws InterruptedException {
    
    info("\n\nChat with the server...");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    
    // Provide a chatter to handle messages back from the server.
    StreamObserver<RouteNote> requestObserver = asyncStub.routeChat(new ClientChatter(finishLatch, this));

    try {
      
      RouteNote[] requests = { newNote("First", 0, 0), newNote("Second", 0, 1), newNote("Third", 1, 0) };
      for (RouteNote r: requests) {
        info("Sending \"{0}\" at {1}, {2}", r.getMessage(), r.getLocation().getLatitude(), r.getLocation().getLongitude());
        requestObserver.onNext(r);
      }
      
    } catch (RuntimeException e) {
      requestObserver.onError(e); // cancel RPC
      throw e;
    }
    
    // Tell the request observer that we're done sending messages, which counts finishLatch down to zero.
    requestObserver.onCompleted();
    finishLatch.await(1, TimeUnit.MINUTES); // Receiving happens asynchronously
  }

  public void info(String msg, Object... params) { logger.log(Level.INFO, msg, params); }

  private RouteNote newNote(String message, int lat, int lon) {
    Point p = Point.newBuilder().setLatitude(lat).setLongitude(lon).build();
    return RouteNote.newBuilder().setMessage(message).setLocation(p).build();
  }

  // Issues several different requests and then exit.
  public static void main(String[] args) throws InterruptedException, MalformedURLException, IOException {
    
    List<Feature> features = RouteGuideUtil.parseFeatures(RouteGuideUtil.getDefaultFeaturesFile());
    RouteGuideClient client = new RouteGuideClient("localhost", 8980);
    
    try {
      client.getFeature(409146138, -746188906);
      client.getFeature(0, 0);
      client.listFeatures(400000000, -750000000, 410000000, -740000000);
      client.recordRoute(features, 10);
      client.routeChat();
    } finally {
      client.shutdown();
    }
    
  }
  
}
