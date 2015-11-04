package org.icrar.awsChiles02.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test the Log4j with Commons Logging
 */
public class TestLog4J {
  private static final Log LOG = LogFactory.getLog(TestLog4J.class);
  public static void main(String[] args) {
    LOG.info("Testing the logging");
  }
}
