package io.memoria.active.eventsourcing;

import io.memoria.active.core.queue.QueueRepo;
import io.memoria.active.eventsourcing.pipeline.AggregateFactory;
import io.memoria.atom.actor.Actor;
import io.memoria.atom.actor.ActorId;
import io.memoria.atom.actor.system.ActorStore;
import io.memoria.atom.actor.system.ActorSystem;
import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.stream.BlockingStream;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.testsuite.eventsourcing.AccountDecider;
import io.memoria.atom.testsuite.eventsourcing.AccountEvolver;
import io.memoria.atom.testsuite.eventsourcing.AccountSaga;
import org.apache.logging.log4j.message.Message;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class AggregateSystemTest {
  private static final int numOfActors = 1000;
  private static final int numOfRequests = 1000;
  private static final AtomicLong atomicLong = new AtomicLong();
  private static final Supplier<Id> idSupplier = () -> Id.of(atomicLong.getAndIncrement());
  private static final Supplier<Long> timeSupplier = System::currentTimeMillis;
  private static final Domain domain = new Domain(new AccountDecider(idSupplier, timeSupplier),
                                                  new AccountEvolver(),
                                                  new AccountSaga(idSupplier, timeSupplier));
  private static final EventRepo eventRepo = new EventRepo(QueueRepo.inMemory(), new SerializableTransformer());
  private static final BlockingStream blockingStream = BlockingStream.inMemory();
  private static final CommandPublisher commandPublisher = new CommandPublisher("command",
                                                                                1,
                                                                                blockingStream,
                                                                                new SerializableTransformer());
  private static final AggregateFactory actorFactory = new AggregateFactory(domain, eventRepo, commandPublisher);

  @ParameterizedTest
  @MethodSource("testArgs")
  void syncTest(ActorStore actorStore, CountDownLatch latch) throws InterruptedException {

    //    System.out.printf("Handling total %d requests", latch.getCount());
    try (var actorSystem = ActorSystem.create(actorStore, actorFactory)) {
      LongStream.range(0, numOfActors).mapToObj(ActorId::new).forEach(actorId -> startActor(actorId, actorSystem));
      latch.await();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void startActor(ActorId actorId, ActorSystem actorSystem) {
    Thread.ofVirtual().start(() -> {
      //      System.out.println("Starting: " + actorId);
      IntStream.range(0, numOfRequests).forEach(_ -> {
        assert actorSystem.apply(actorId, new Message()).isSuccess();
      });
    });
  }

  private static ActorStore cachedActorStore() {
    var config = new MutableConfiguration<ActorId, Actor>().setTypes(ActorId.class, Actor.class).setStoreByValue(false);
    //.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE));
    var cache = Caching.getCachingProvider().getCacheManager().createCache("simpleCache", config);
    return ActorStore.cacheStore(cache);
  }

  private static ActorStore mapActorStore() {
    return ActorStore.mapStore(new ConcurrentHashMap<>());
  }

  private static Stream<Arguments> testArgs() {
    return Stream.of(Arguments.of(mapActorStore(), createLatch()), Arguments.of(cachedActorStore(), createLatch()));
  }

  private static CountDownLatch createLatch() {
    return new CountDownLatch(numOfActors * numOfRequests);
  }
}
