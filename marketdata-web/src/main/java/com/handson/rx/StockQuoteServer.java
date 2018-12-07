package com.handson.rx;

import com.handson.dto.Quote;
import com.handson.infra.EventStreamClient;
import com.handson.infra.HttpRequest;
import com.handson.infra.RxNettyEventServer;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.subjects.BehaviorSubject;

import java.util.concurrent.TimeUnit;


public class StockQuoteServer extends RxNettyEventServer<Quote> {

    private final EventStreamClient stockQuoteEventStreamClient;
    private final EventStreamClient forexEventStreamClient;
    private final Scheduler scheduler;

    public StockQuoteServer(int port,
                            EventStreamClient stockQuoteEventStreamClient,
                            EventStreamClient forexEventStreamClient,
                            Scheduler scheduler) {
        super(port);
        this.stockQuoteEventStreamClient = stockQuoteEventStreamClient;
        this.forexEventStreamClient = forexEventStreamClient;
        this.scheduler = scheduler;
    }

    @Override
    protected Observable<Quote> getEvents(HttpRequest request) {
        String stockCode = request.getParameter("code");
        Observable<Quote> forex$ = this.forexEventStreamClient
                .readServerSideEvents()
                .map(Quote::fromJson);

        BehaviorSubject<Quote> quoteSubject = BehaviorSubject.create();
        Subscription forexSubscription = forex$.subscribe(quoteSubject);

        return stockQuoteEventStreamClient
            .readServerSideEvents()
            .map(Quote::fromJson)
            .filter(q -> q.code.equals(stockCode))
            .flatMap(stockQuote -> quoteSubject.take(1)
                    .map(forexQuote -> new Quote(stockCode, stockQuote.quote / forexQuote.quote)))
            .doOnUnsubscribe(forexSubscription::unsubscribe)    ;
    }
}
