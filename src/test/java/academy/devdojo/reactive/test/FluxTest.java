package academy.devdojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Flow;

@Slf4j
public class FluxTest {

    @Test
    public void fluxSubscriber() {
        Flux<String> fluxString = Flux.just("José", "Alfredo", "Gubee")
                .log();

        StepVerifier.create(fluxString)
                .expectNext("José", "Alfredo", "Gubee")
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbers() {
        Flux<Integer> fluxInteger = Flux.range(1, 7)
                .log();

        fluxInteger.subscribe(i -> log.info("Number {}", i));

        log.info("==============================================");

        StepVerifier.create(fluxInteger)
                .expectNext(1, 2, 3, 4, 5, 6, 7)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberFromList() {
        Flux<Integer> fluxFromList = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
                .log();

        fluxFromList.subscribe(i -> log.info("Number {}", i));

        log.info("==============================================");

        StepVerifier.create(fluxFromList)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersError() {
        Flux<Integer> fluxFromList = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
                .log()
                .map(i -> {
                    if(i == 4) {
                        throw new IndexOutOfBoundsException("Index out of bounds");
                    }
                    return i;
                });

        fluxFromList.subscribe(i -> log.info("Number {}", i), Throwable::printStackTrace,
                () -> log.info("DONE!"), subscription -> subscription.request(3));

        log.info("==============================================");

        StepVerifier.create(fluxFromList)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberNumbersNotSoUglyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log();

        flux.subscribe(new BaseSubscriber<Integer>() {
            public int count = 0;
            public final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(2);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if(count >= 2) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        log.info("==============================================");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }


    @Test
    public void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
                .take(10)
                .log();

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);
    }

    @Test
    public void fluxSubscriberIntervalTwo(){
        StepVerifier.withVirtualTime(this::createInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }


    @Test
    public void fluxSubscriberPrettyBackpressure() {
        Flux<Integer> fluxInteger = Flux.range(1, 10)
                .log()
                .limitRate(3);

        fluxInteger.subscribe(i -> log.info("Number {}", i));

        log.info("==============================================");

        StepVerifier.create(fluxInteger)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void connectableFlux() throws InterruptedException{
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        connectableFlux.connect();

        log.info("Thread sleeping for 300ms");

        Thread.sleep(300);

        connectableFlux.subscribe(i -> log.info("Sub1 number {}", i));

        log.info("Thread sleeping for 200ms");

        Thread.sleep(200);

        connectableFlux.subscribe(i -> log.info("Sub2 number {}", i));
    }

    @Test
    public void connectableFluxAutoConnect() throws InterruptedException{
        Flux<Integer> connectableFlux = Flux.range(1, 5)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(2);

        StepVerifier
                .create(connectableFlux)
                .then(connectableFlux::subscribe)
                .then(connectableFlux::subscribe)
                .expectNext(1, 2, 3, 4, 5)
                .expectComplete()
                .verify();
    }


    //Método, não teste
    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1))
                .log();
    }

}

