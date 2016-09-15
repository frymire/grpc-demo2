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
  private final RouteGuideBlockingStub blockingServerStub;
  private final RouteGuideStub asynchServerStub;

  private final Logger logger = Logger.getLogger(RouteGuideClient.class.getName());

  // Construct client for accessing RouteGuide server at host:port.
  public RouteGuideClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
    blockingServerStub = RouteGuideGrpc.newBlockingStub(channel);
    asynchServerStub = RouteGuideGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  // Blocking unary call example.  Calls getFeature and prints the response.
  public void getFeature(int lat, int lon) {
    
    info("*** GetFeature: lat={0} lon={1}", lat, lon);

    Point locationRequest = Point.newBuilder().setLatitude(lat).setLongitude(lon).build();

    Feature featureResponse;
    try {
      featureResponse = blockingServerStub.getFeature(locationRequest);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    
    if (RouteGuideUtil.exists(featureResponse)) {
      info("Found feature called \"{0}\" at {1}, {2}",
          featureResponse.getName(),
          RouteGuideUtil.getLatitude(featureResponse.getLocation()),
          RouteGuideUtil.getLongitude(featureResponse.getLocation()));
    } else {
      info("Found no feature at {0}, {1}",
          RouteGuideUtil.getLatitude(featureResponse.getLocation()),
          RouteGuideUtil.getLongitude(featureResponse.getLocation()));
    }
    
  }

  /** 
   * Blocking server-streaming example. Requests all features within a rectangle  
   * of interest from the server and prints each response as it arrives.
   */
  public void listFeatures(int lowLat, int lowLon, int hiLat, int hiLon) {
    
    info("Get features in a rectangle...\nlowLat={0} lowLon={1} hiLat={2} hiLon={3}", lowLat, lowLon, hiLat, hiLon);

    Point lowPoint = Point.newBuilder().setLatitude(lowLat).setLongitude(lowLon).build();
    Point highPoint = Point.newBuilder().setLatitude(hiLat).setLongitude(hiLon).build();
    Rectangle request = Rectangle.newBuilder().setLo(lowPoint).setHi(highPoint).build();
    
    Iterator<Feature> featureResponses;
    try {
      featureResponses = blockingServerStub.listFeatures(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }

    StringBuilder responseLog = new StringBuilder("Result: ");
    while (featureResponses.hasNext()) responseLog.append(featureResponses.next());
    info(responseLog.toString());
  }

  // Async client-streaming example. Sends numPoints randomly chosen points from features with 
  // a variable delay in between. Prints the statistics when they are sent from the server.
  public void recordRoute(List<Feature> features, int numPoints) throws InterruptedException {
    
    info("Record a route...");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    
    // Set up a client-side observer to handle responses from the server. Pass it to the 
    // server stub to get a reference to the server-side request observer.
    StreamObserver<RouteSummary> responseObserver = new RouteSummaryListener(this, logger, finishLatch);
    StreamObserver<Point> requestObserver = asynchServerStub.recordRoute(responseObserver);

    // Send randomly selected from the features list to the request observer, pausing for a bit after each one.
    try {
      
      Random rand = new Random();
      for (int i = 0; i < numPoints; ++i) {
        int index = rand.nextInt(features.size());
        Point point = features.get(index).getLocation();
        info("Visiting point {0}, {1}", RouteGuideUtil.getLatitude(point), RouteGuideUtil.getLongitude(point));
        requestObserver.onNext(point);
        Thread.sleep(rand.nextInt(250) + 250);
        
        // Any requests sent after RPC completed or throws an error would just be thrown away.
        if (finishLatch.getCount() == 0) return;        
      }
      
    } catch (RuntimeException e) {
      requestObserver.onError(e); // Cancel RPC
      throw e;
    }
    
    // Mark the end of requests, and wait for a response. Once the responseObserver receives    
    // a response, it will count down the finish latch and release this thread. 
    requestObserver.onCompleted();
    finishLatch.await(1, TimeUnit.MINUTES);
  }

  // Bi-directional example, which can only be asynchronous. 
  // Send some chat messages, and print any messages received from the server.
  public void routeChat() throws InterruptedException {
    
    info("\n\nChat with the server...");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    
    // Provide a chatter to handle messages back from the server.
    StreamObserver<RouteNote> requestObserver = asynchServerStub.routeChat(new ClientChatter(finishLatch, this));

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
