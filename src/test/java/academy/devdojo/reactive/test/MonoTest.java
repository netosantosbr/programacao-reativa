package academy.devdojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
public class MonoTest {

    @Test
    public void monoSubscriber() {
        String name = "José Alfredo";

        //Publisher
        Mono<String> mono = Mono.just(name)
                .log();

        //Subscription
        mono.subscribe();

        //Teste propriamente dito
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void subscriberConsumer() {
        String name = "José Alfredo";

        //Publisher
        Mono<String> mono = Mono.just(name)
                .log();

        //Subscription
        mono.subscribe(s -> log.info("Value {}", s));

        //Teste propriamente dito
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();

    }

    @Test
    public void subscriberConsumerError() {
        String name = "José Alfredo";

        Mono<String> mono = Mono.just(name)
                .map(s -> {throw new RuntimeException("Testing mono with error");});

        mono.subscribe(s -> log.info("Name {}", s), s -> log.error("Something bad happened"));
        mono.subscribe(s -> log.info("Name {}", s), Throwable::printStackTrace);

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();

    }

    @Test
    public void subscriberConsumerComplete() {
        String name = "José Alfredo";

        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
                Throwable::printStackTrace, () -> log.info("FINISHED!"));

        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();

    }

    @Test
    public void subscriberConsumerSubscription() {
        String name = "José Alfredo";

        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(longNumber -> log.info("Request received, starting doing something..."))
                .doOnNext(s -> log.info("Value is here. Executing onNext() {} ", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Value is here. Executing onNext() {} ", s))
                .doOnSuccess(s -> log.info("doOnSucess executed"));


        mono.subscribe(s -> log.info("Value {}", s),
                Throwable::printStackTrace, () -> log.info("FINISHED!"),
                subscription -> subscription.request(5L));

    }

    @Test
    public void monoDoOnError() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
                .doOnError(e -> log.error("Error message {}", e.getMessage()))
                .doOnNext(s -> log.info("Executing this doOnNext()"))
                .log();


        StepVerifier.create(error)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    public void monoDoOnErrorResume() {
        String name = "José Alfredo";

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
                .onErrorReturn("EMPTY")
                .onErrorResume(s -> {
                    log.info("Inside on error resume");
                    return Mono.just(name);
                })
                .doOnError(e -> log.error("Error message {}", e.getMessage()))
                .log();


        StepVerifier.create(error)
                .expectNext("EMPTY")
                .verifyComplete();
    }
}
