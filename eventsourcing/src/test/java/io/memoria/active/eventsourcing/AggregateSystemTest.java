package io.memoria.active.eventsourcing;

import io.memoria.active.eventsourcing.pipeline.AggregateFactory;
import io.memoria.atom.actor.Actor;
import io.memoria.atom.actor.ActorId;
import io.memoria.atom.actor.system.ActorStore;
import io.memoria.atom.actor.system.ActorSystem;
import io.memoria.atom.core.id.Id;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.CommandMeta;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.AccountDecider;
import io.memoria.atom.testsuite.eventsourcing.AccountEvolver;
import io.memoria.atom.testsuite.eventsourcing.AccountSaga;
import io.memoria.atom.testsuite.eventsourcing.command.CreateAccount;
import io.memoria.atom.testsuite.eventsourcing.command.Credit;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class AggregateSystemTest {
  private static final int numOfActors = 10;
  private static final AtomicLong atomicLong = new AtomicLong();
  private static final Supplier<Id> idSupplier = () -> Id.of(atomicLong.getAndIncrement());
  private static final Supplier<Long> timeSupplier = System::currentTimeMillis;
  private static final Domain domain = new Domain(new AccountDecider(idSupplier, timeSupplier),
                                                  new AccountEvolver(),
                                                  new AccountSaga(idSupplier, timeSupplier));

  @ParameterizedTest
  @MethodSource("testArgs")
  void syncTest(ActorStore actorStore) {
    // System.out.println("Start actor system");
    var eventRepo = EventRepo.inMemory();
    var commandRepo = CommandRepo.inMemory();
    var actorFactory = new AggregateFactory(domain, eventRepo, commandRepo);
    try (var actorSystem = ActorSystem.create(actorStore, actorFactory)) {
      createActorIds().forEach(actorId -> startActor(actorId, actorSystem));
      createActorIds().forEach(actorId -> {
        Awaitility.await().until(() -> {
          var size = eventRepo.size(StateId.of(actorId.value())).get();
          // System.out.printf("size: Actor: %s size: %d \n", actorId.value(), size);
          return size == 2;
        });
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stream<Arguments> testArgs() {
    return Stream.of(Arguments.of(mapActorStore()), Arguments.of(cachedActorStore()));
  }

  private static Stream<ActorId> createActorIds() {
    return LongStream.range(0, numOfActors).mapToObj(ActorId::new);
  }

  private static void startActor(ActorId actorId, ActorSystem actorSystem) {
    Thread.ofVirtual().start(() -> {
      assert actorSystem.apply(actorId, createCommand(actorId.value())).isSuccess();
      assert actorSystem.apply(actorId, addBalance(actorId.value())).isSuccess();
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

  private static CreateAccount createCommand(String stateId) {
    var meta = new CommandMeta(CommandId.of(UUID.randomUUID()), StateId.of(stateId));
    return new CreateAccount(meta, stateId, 500);
  }

  private static Credit addBalance(String stateId) {
    var meta = new CommandMeta(CommandId.of(UUID.randomUUID()), StateId.of(stateId));
    return new Credit(meta, StateId.of("the_bank"), 500);
  }
}
