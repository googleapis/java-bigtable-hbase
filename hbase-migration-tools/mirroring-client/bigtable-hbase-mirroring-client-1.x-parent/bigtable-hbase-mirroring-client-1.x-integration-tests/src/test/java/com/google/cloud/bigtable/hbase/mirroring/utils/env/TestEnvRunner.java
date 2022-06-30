package com.google.cloud.bigtable.hbase.mirroring.utils.env;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.InvalidTestClassError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

public class TestEnvRunner extends Suite {
  private final Constructor<?> constructor;

  public TestEnvRunner(Class<?> klass) throws Throwable {
    this(klass, new RunnersFactory(klass, TestEnvServers.getInstance()));
  }

  private TestEnvRunner(Class<?> klass, RunnersFactory builder) throws Exception {
    super(klass, builder.createRunners());

    try {
      constructor = klass.getConstructor(TestEnv.class);
    } catch (NoSuchMethodException e) {
      throw new InvalidTestClassError(klass, Collections.<Throwable>singletonList(
          new Exception("The test class " + klass.getName()
              + " must have a constructor which takes a single argument of a TestEnv")
      ));
    }
  }

  private static class RunnersFactory {
    private final TestClass testClass;
    private final TestEnvServers testEnvServers;

    public RunnersFactory(Class<?> klass,
        TestEnvServers testEnvServers) {
      testClass = new TestClass(klass);
      this.testEnvServers = testEnvServers;
    }

    public List<Runner> createRunners() throws Exception {
      List<TestEnv> testEnvs = Arrays.asList(
          new TestEnv(testEnvServers, TestEnv.Type.HBASE_MINICLUSTER, TestEnv.Type.BIGTABLE),
          new TestEnv(testEnvServers, TestEnv.Type.BIGTABLE, TestEnv.Type.HBASE_MINICLUSTER)
      );
      List<Runner> runners = new ArrayList<>();
      for (TestEnv testEnv : testEnvs) {
        runners.add(new SingleTestRunner(testClass, testEnvServers, testEnv));
      }
      return runners;
    }
  }

  private static class SingleTestRunner extends BlockJUnit4ClassRunner {

    private final TestEnvServers testEnvServers;
    private final TestEnv testEnv;

    public SingleTestRunner(TestClass testClass, TestEnvServers testEnvServers, TestEnv testEnv) throws InitializationError {
      super(testClass);
      this.testEnvServers = testEnvServers;
      this.testEnv = testEnv;
    }

    @Override
    protected void validateConstructor(List<Throwable> errors) {}

    @Override
    protected Object createTest() throws Exception {
      Constructor<?> constructor = getTestClass().getOnlyConstructor();
      return constructor.newInstance(testEnv);
    }

    @Override
    protected String getName() {
      return testEnv.getName();
    }

    @Override
    protected String testName(FrameworkMethod method) {
      return String.format("%s[%s]", method.getName(), getName());
    }

    @Override
    protected Statement classBlock(RunNotifier notifier) {
      final Statement childrenBlock = childrenInvoker(notifier);

      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          testEnvServers.retain();
          try {
            childrenBlock.evaluate();
          } finally {
            testEnvServers.release();
          }
        }
      };
    }

    @Override
    protected Annotation[] getRunnerAnnotations() {
      return new Annotation[0];
    }
  }
}
