package io.grpc.examples.routeguide;

import static java.lang.Math.max;
import static java.lang.Math.min;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.logging.Logger;

/** A sample gRPC server that serves the RouteGuide (see route_guide.proto) service. */
public class RouteGuideServer {

  private static final Logger logger = Logger.getLogger(RouteGuideServer.class.getName());
  private final int port;
  private final Server server;

  /** Create a RouteGuide server using serverBuilder as a base and features as data. */
  public RouteGuideServer(ServerBuilder<?> serverBuilder, int port, Collection<Feature> features) {
    this.port = port;
    server = serverBuilder.addService(new RouteGuideService(features)).build();
  }

  /** Create a RouteGuide server listening on a given port, using a database in a specified feature file. */
  public RouteGuideServer(int port, URL featureFile) throws IOException {
    this(ServerBuilder.forPort(port), port, RouteGuideUtil.parseFeatures(featureFile));
  }

  public RouteGuideServer(int port) throws IOException {
    this(port, RouteGuideUtil.getDefaultFeaturesFile());
  }

  /** Start serving requests. */
  public void start() throws IOException {

    server.start();
    logger.info("Server started, listening on " + port);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        RouteGuideServer.this.stop();
        System.err.println("*** server shut down");
      }
    });

  }

  /** Stop serving requests and shutdown resources. */
  public void stop() {
    if (server != null) server.shutdown();
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) server.awaitTermination();
  }


  public static class RouteGuideService extends RouteGuideGrpc.RouteGuideImplBase {
    
    private final Collection<Feature> features;
    
    RouteGuideService(Collection<Feature> features) { this.features = features; }

    /** Returns the feature at the specified point, or an unnamed feature if it doesn't already exist. */
    public Feature checkFeature(Point location) {
      for (Feature feature : features) {
        boolean latitudesMatch = feature.getLocation().getLatitude() == location.getLatitude();
        boolean longitudesMatch = feature.getLocation().getLongitude() == location.getLongitude();
        if (latitudesMatch && longitudesMatch) return feature;
      }

      // No feature was found, return an unnamed feature.
      return Feature.newBuilder().setName("").setLocation(location).build();
    }

    

    /**
     * Returns the Feature at the requested Point. If no feature at that location
     * exists, an unnamed feature is returned at the provided location.
     *
     * @param request the requested location for the feature.
     * @param responseObserver the observer that will receive the feature at the requested point.
     */
    @Override
    public void getFeature(Point request, StreamObserver<Feature> responseObserver) {
      responseObserver.onNext(checkFeature(request));
      responseObserver.onCompleted();
    }

    /**
     * Gets all features contained within the given bounding Rectangle.
     *
     * @param request the bounding rectangle for the requested features.
     * @param responseObserver the observer that will receive the features.
     */
    @Override
    public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver) {

      int left = min(request.getLo().getLongitude(), request.getHi().getLongitude());
      int right = max(request.getLo().getLongitude(), request.getHi().getLongitude());
      int top = max(request.getLo().getLatitude(), request.getHi().getLatitude());
      int bottom = min(request.getLo().getLatitude(), request.getHi().getLatitude());

      for (Feature feature : features) {        
        if (!RouteGuideUtil.exists(feature)) continue;
        int lat = feature.getLocation().getLatitude();
        int lon = feature.getLocation().getLongitude();
        if (lon >= left && lon <= right && lat >= bottom && lat <= top) { responseObserver.onNext(feature); }
      }

      responseObserver.onCompleted();
    }
    
    


    /**
     * Gets a stream of points, and responds with statistics about the "trip": number of points,
     * number of known features visited, total distance traveled, and total time spent.
     *
     * @param responseObserver an observer to receive the response summary.
     * @return an observer to receive the requested route points.
     */
    @Override
    public StreamObserver<Point> recordRoute(final StreamObserver<RouteSummary> responseObserver) {
      return new TripSummarizer(responseObserver, logger, this);
    }
    

    /**
     * Receives a stream of message/location pairs, and responds with a stream of all previous
     * messages at each of those locations.
     *
     * @param responseObserver an observer to receive the stream of previous messages.
     * @return an observer to handle requested message/location pairs.
     */
    @Override
    public StreamObserver<RouteNote> routeChat(final StreamObserver<RouteNote> responseObserver) {
      return new RouteChatter(responseObserver, logger);
    }

  } // class RouteGuideService

  
  public static void main(String[] args) throws Exception {
    RouteGuideServer server = new RouteGuideServer(8980);
    server.start();
    server.blockUntilShutdown();
  }

}
