// UNCLASSIFIED

package io.grpc.examples.routeguide;

import java.util.concurrent.CountDownLatch;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class ClientChatter implements StreamObserver<RouteNote> {
  
  RouteGuideClient rgc;
  CountDownLatch finishLatch;
  
  public ClientChatter(CountDownLatch finishLatch, RouteGuideClient rgc) {
    this.rgc = rgc;
    this.finishLatch = finishLatch;
  }

  @Override public void onNext(RouteNote note) {
    rgc.info("Got message \"{0}\" at {1}, {2}", note.getMessage(), 
        note.getLocation().getLatitude(), note.getLocation().getLongitude());
  }

  @Override public void onError(Throwable t) {
    System.err.println("RouteChat Failed: " + Status.fromThrowable(t));
    finishLatch.countDown();
  }

  @Override public void onCompleted() {
    rgc.info("Finished RouteChat");
    finishLatch.countDown();
  }
}
