// UNCLASSIFIED

package io.grpc.examples.routeguide;

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.stub.StreamObserver;

public class RouteSummarizer implements StreamObserver<Point> {
  
  StreamObserver<RouteSummary> responseObserver;
  Logger logger;
  RouteGuideServer.RouteGuideService rgs;
  
  int pointCount;
  int featureCount;
  int distance;
  Point previous;
  long startTime = System.nanoTime();
  
  public RouteSummarizer(
          StreamObserver<RouteSummary> responseObserver,
          Logger logger,
          RouteGuideServer.RouteGuideService rgs) {
    this.responseObserver = responseObserver;
    this.logger = logger;
    this.rgs = rgs;
  }

  // For each point after the first, add the incremental distance from the previous point to the total distance.
  @Override public void onNext(Point point) {
    pointCount++;
    if (RouteGuideUtil.exists(rgs.checkFeature(point))) featureCount++;
    if (previous != null) distance += calcDistance(previous, point);
    previous = point;
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
  
  @Override public void onError(Throwable t) { logger.log(Level.WARNING, "recordRoute cancelled"); }

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