// UNCLASSIFIED

package io.grpc.examples.routeguide;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.stub.StreamObserver;


public class RouteChatter implements StreamObserver<RouteNote> {
  
  private StreamObserver<RouteNote> responseObserver;
  private Logger logger;
  private final ConcurrentMap<Point, List<RouteNote>> routeNotes = new ConcurrentHashMap<Point, List<RouteNote>>();
  
  public RouteChatter(StreamObserver<RouteNote> responseObserver, Logger logger) {
    this.responseObserver = responseObserver;
    this.logger = logger;
  }
  
  // Respond with all previous notes at this location, and add the new note to the list
  @Override public void onNext(RouteNote note) {          
    List<RouteNote> notes = getOrCreateNotes(note.getLocation());
    for (RouteNote prevNote : notes.toArray(new RouteNote[0])) { responseObserver.onNext(prevNote); }
    notes.add(note);
  }

  @Override public void onError(Throwable t) { logger.log(Level.WARNING, "routeChat cancelled"); }
  
  @Override public void onCompleted() { responseObserver.onCompleted(); }
  
  /** Get the notes list for the given location. If missing, create it. */
  private List<RouteNote> getOrCreateNotes(Point location) {
    List<RouteNote> notes = Collections.synchronizedList(new ArrayList<RouteNote>());
    List<RouteNote> prevNotes = routeNotes.putIfAbsent(location, notes);
    return prevNotes != null ? prevNotes : notes;
  }
  
}
