// UNCLASSIFIED

package io.grpc.examples.routeguide;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.stub.StreamObserver;


public class ServerChatter implements StreamObserver<RouteNote> {
  
  private final StreamObserver<RouteNote> responseObserver;
  private final Logger logger;
  private final ConcurrentMap<Point, List<RouteNote>> routeNotes;
  
  public ServerChatter(
      StreamObserver<RouteNote> responseObserver, 
      Logger logger, 
      ConcurrentMap<Point, List<RouteNote>> routeNotes) {
    this.responseObserver = responseObserver;
    this.logger = logger;
    this.routeNotes = routeNotes;
  }
  
  /** Respond with all previous notes at this location, then add the new note to the list. */
  @Override public void onNext(RouteNote note) {          
    List<RouteNote> previousNotes = getNotesForLocation(note.getLocation());
    for (RouteNote n: previousNotes) { responseObserver.onNext(n); }
    previousNotes.add(note);
  }

  /** Get the notes list for the given location. If missing, create it. */
  private List<RouteNote> getNotesForLocation(Point location) {
    List<RouteNote> notes = Collections.synchronizedList(new ArrayList<>());
    List<RouteNote> prevNotes = routeNotes.putIfAbsent(location, notes);
    return prevNotes != null ? prevNotes : notes;
  }

  @Override public void onCompleted() { responseObserver.onCompleted(); }
  
  @Override public void onError(Throwable t) { logger.log(Level.WARNING, "Encountered error in routeChat", t); }
  
}
