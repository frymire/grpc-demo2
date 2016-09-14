
package io.grpc.examples.routeguide;

import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class RouteGuideUtil {
  
  private static final double COORD_FACTOR = 1e7;
  public static double getLatitude(Point p) { return p.getLatitude() / COORD_FACTOR; }
  public static double getLongitude(Point p) { return p.getLongitude() / COORD_FACTOR; }

  public static URL getDefaultFeaturesFile() throws MalformedURLException {
//    return RouteGuideServer.class.getResource("route_guide_db.json");
    return new java.io.File("src/main/resources/route_guide_db.json").toURI().toURL();
  }

  public static List<Feature> parseFeatures(URL file) throws IOException {
    try (InputStream input = file.openStream(); Reader reader = new InputStreamReader(input)) {
      FeatureDatabase.Builder database = FeatureDatabase.newBuilder();
      JsonFormat.parser().merge(reader, database);
      return database.getFeatureList();
    }
  }

  public static boolean exists(Feature feature) {
    return (feature != null) && (!feature.getName().isEmpty());
  }
}
