package com.handson.rx;


import com.handson.dto.Quote;
import com.handson.dto.Trade;
import com.handson.dto.Vwap;
import com.handson.infra.EventStreamClient;
import com.handson.infra.HttpRequest;
import com.handson.infra.RxNettyEventServer;
import rx.Observable;
import rx.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class VwapServer extends RxNettyEventServer<Vwap> {

    private final EventStreamClient tradeEventStreamClient;
    private final Scheduler scheduler;

    public VwapServer(int port, EventStreamClient tradeEventStreamClient, Scheduler scheduler) {
        super(port);
        this.tradeEventStreamClient = tradeEventStreamClient;
        this.scheduler = scheduler;
    }

    @Override
    protected Observable<Vwap> getEvents(HttpRequest request) {
        /* Etape 0
        String stockCode = request.getParameter("code");
        return Observable.never();
        */

        /* Etape 1 -  filtre sur code de la stock et objet vwap
        String stockCode = request.getParameter("code");
        return tradeEventStreamClient
                .readServerSideEvents()
                .map(Trade::fromJson)
                .filter(t -> t.code.equals(stockCode))
                .map(t -> new Vwap(t.code, t.nominal/t.quantity, t.quantity));
        */

        /* Etape 2 - calcul du vwap
        String stockCode = request.getParameter("code");
        return tradeEventStreamClient
                .readServerSideEvents()
                .map(Trade::fromJson)
                .filter(t -> t.code.equals(stockCode))
                .scan(new Vwap(), (v, t) -> {
                    double volume = v.volume + t.quantity;
                    double vwap = (v.volume * v.vwap + t.nominal) / volume;
                    return new Vwap(t.code, vwap, volume);
                }).skip(1);*/

        /* etape 3 avec sampling */
        String stockCode = request.getParameter("code");
        return tradeEventStreamClient
                .readServerSideEvents()
                .map(Trade::fromJson)
                .filter(t -> t.code.equals(stockCode))
                .scan(new Vwap(), (v, t) -> v.addTrade(t))
                .skip(1)
                .sample(1, TimeUnit.SECONDS, scheduler);
    }
}
