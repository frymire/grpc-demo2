// UNCLASSIFIED

package io.grpc.examples.routeguide;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class RouteSummaryListener implements StreamObserver<RouteSummary> {
  
  RouteGuideClient rgc;
  Logger logger;
  CountDownLatch finishLatch;
  
  public RouteSummaryListener(RouteGuideClient rgc, Logger logger, CountDownLatch finishLatch) {
    this.rgc = rgc;
    this.logger = logger;
    this.finishLatch = finishLatch;
  }

  @Override public void onNext(RouteSummary summary) {
    rgc.info("Finished trip with {0} points. Passed {1} features. Travelled {2} meters. It took {3} seconds.", 
        summary.getPointCount(), summary.getFeatureCount(), summary.getDistance(), summary.getElapsedTime());
  }

  @Override public void onCompleted() {
    rgc.info("Finished RecordRoute");
    finishLatch.countDown();
  }

  @Override public void onError(Throwable t) {
    Status status = Status.fromThrowable(t);
    logger.log(Level.WARNING, "RecordRoute Failed: {0}", status);
    finishLatch.countDown();
  }
  
}
