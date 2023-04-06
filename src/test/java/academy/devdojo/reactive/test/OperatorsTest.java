package academy.devdojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class OperatorsTest {

    @Test
    public void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map 1  - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.elastic())
                .map(i -> {
                    log.info("Map 2  - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map 1  - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2  - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void subscribeOnIO() throws InterruptedException{
        Mono<List<String>> mono = Mono.fromCallable( () -> Files.readAllLines(Path.of("text-file")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        mono.subscribe(s -> log.info("{}", s));

        Thread.sleep(2000);

        StepVerifier.create(mono)
                .expectSubscription()
                .thenConsumeWhile( list -> {
                    Assertions.assertFalse(list.isEmpty());
                    log.info("Size {}", list.size());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    public void switchIfEmptyOperator() {
        String message = "not empty anymore";

        Flux<Object> flux = emptyFlux()
                .switchIfEmpty(Flux.just(message))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(message)
                .expectComplete()
                .verify();
    }

    @Test
    public void deferOperator() throws Exception {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));


        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);

    }

    @Test
    public void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(flux1, flux2)
                .log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    public void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = flux1.concatWith(flux2)
                .log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    public void combineLatestOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combineLatest = Flux.combineLatest(flux1, flux2,
                (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
                        .log();

        StepVerifier.create(combineLatest)
                .expectSubscription()
                .expectNext("BC", "BD")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.merge(flux1, flux2)
                .log();

        mergeFlux.subscribe(log::info);

        Thread.sleep(2000);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeWithOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeWithFlux = flux1.mergeWith(flux2)
                .log();


        StepVerifier.create(mergeWithFlux)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeSequentialOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeWithFlux = Flux.mergeSequential(flux1, flux2, flux1)
                .log();


        StepVerifier.create(mergeWithFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "a", "b")
                .expectComplete()
                .verify();
    }

    @Test
    public void concatOperatorError() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if(s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeWithFlux = Flux.concat(flux1, flux2)
                .log();


        StepVerifier.create(mergeWithFlux)
                .expectSubscription()
                .expectNext("a")
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    public void mergeDelayErrorOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if(s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeWithFlux = Flux.mergeDelayError(1, flux1, flux2, flux1)
                .log();

        mergeWithFlux.subscribe(log::info);


        StepVerifier.create(mergeWithFlux)
                .expectSubscription()
                .expectNext("a", "c", "d", "a")
                .expectError()
                .verify();
    }


    @Test
    public void flatMapOperator() throws InterruptedException {
        Flux<String> flux = Flux.just("a", "b");

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMap(this::findByName)
                .log();

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();
    }

    public Flux<String> findByName(String name) {
        return name.equals("A") ? Flux.just("nameA1", "nameA2") : Flux.just("nameB1", "nameB2");
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }
}
