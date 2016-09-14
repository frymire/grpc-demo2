package io.grpc.examples.routeguide;

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A sample gRPC server that serves the RouteGuide (see route_guide.proto) service.
 */
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

  /**
   * Main method.  This comment makes the linter happy.
   */
  public static void main(String[] args) throws Exception {
    RouteGuideServer server = new RouteGuideServer(8980);
    server.start();
    server.blockUntilShutdown();
  }

  /**
   * Our implementation of RouteGuide service.
   *
   * <p>See route_guide.proto for details of the methods.
   */
  private static class RouteGuideService extends RouteGuideGrpc.RouteGuideImplBase {
    
    private final Collection<Feature> features;
    private final ConcurrentMap<Point, List<RouteNote>> routeNotes = new ConcurrentHashMap<Point, List<RouteNote>>();

    RouteGuideService(Collection<Feature> features) { this.features = features; }

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
      
      return new StreamObserver<Point>() {
        
        int pointCount;
        int featureCount;
        int distance;
        Point previous;
        long startTime = System.nanoTime();

        // For each point after the first, add the incremental distance from the previous point to the total distance.
        @Override public void onNext(Point point) {
          pointCount++;
          if (RouteGuideUtil.exists(checkFeature(point))) featureCount++;
          if (previous != null) distance += calcDistance(previous, point);
          previous = point;
        }

        @Override public void onError(Throwable t) {
          logger.log(Level.WARNING, "recordRoute cancelled");
        }

        @Override public void onCompleted() {

          long seconds = NANOSECONDS.toSeconds(System.nanoTime() - startTime);

          RouteSummary summary = RouteSummary.newBuilder()
              .setPointCount(pointCount)
              .setFeatureCount(featureCount)
              .setDistance(distance)
              .setElapsedTime((int) seconds)
              .build();

          responseObserver.onNext(summary);
          responseObserver.onCompleted();
        }

      }; // new StreamObserver<Point>()
      
    } // recordRoute
    

    /**
     * Receives a stream of message/location pairs, and responds with a stream of all previous
     * messages at each of those locations.
     *
     * @param responseObserver an observer to receive the stream of previous messages.
     * @return an observer to handle requested message/location pairs.
     */
    @Override
    public StreamObserver<RouteNote> routeChat(final StreamObserver<RouteNote> responseObserver) {
      
      return new StreamObserver<RouteNote>() {
        
        // Respond with all previous notes at this location, and add the new note to the list
        @Override public void onNext(RouteNote note) {          
          List<RouteNote> notes = getOrCreateNotes(note.getLocation());
          for (RouteNote prevNote : notes.toArray(new RouteNote[0])) { responseObserver.onNext(prevNote); }
          notes.add(note);
        }

        @Override public void onError(Throwable t) { logger.log(Level.WARNING, "routeChat cancelled"); }
        
        @Override public void onCompleted() { responseObserver.onCompleted(); }
      };
      
    } // routeChat

    
    /** Get the notes list for the given location. If missing, create it. */
    private List<RouteNote> getOrCreateNotes(Point location) {
      List<RouteNote> notes = Collections.synchronizedList(new ArrayList<RouteNote>());
      List<RouteNote> prevNotes = routeNotes.putIfAbsent(location, notes);
      return prevNotes != null ? prevNotes : notes;
    }

    /** Returns the feature at the specified point, or an unnamed feature if it doesn't already exist. */
    private Feature checkFeature(Point location) {
      for (Feature feature : features) {
        boolean latitudesMatch = feature.getLocation().getLatitude() == location.getLatitude();
        boolean longitudesMatch = feature.getLocation().getLongitude() == location.getLongitude();
        if (latitudesMatch && longitudesMatch) return feature;
      }

      // No feature was found, return an unnamed feature.
      return Feature.newBuilder().setName("").setLocation(location).build();
    }

    /**
     * Returns the distance between two points in meters, as computed using
     * "haversine" formula. See http://www.movable-type.co.uk/scripts/latlong.html.
     */
    private static double calcDistance(Point start, Point end) {
      
      double lat1 = RouteGuideUtil.getLatitude(start);
      double lat2 = RouteGuideUtil.getLatitude(end);
      double lon1 = RouteGuideUtil.getLongitude(start);
      double lon2 = RouteGuideUtil.getLongitude(end);
      double phi1 = toRadians(lat1);
      double phi2 = toRadians(lat2);
      double deltaLat = toRadians(lat2 - lat1);
      double deltaLon = toRadians(lon2 - lon1);

      double a = sin(deltaLat / 2) * sin(deltaLat / 2) + cos(phi1) * cos(phi2) * sin(deltaLon / 2) * sin(deltaLon / 2);
      double c = 2 * atan2(sqrt(a), sqrt(1 - a));

      int r = 6371000; // metres
      return r * c;
    }
  }
}
