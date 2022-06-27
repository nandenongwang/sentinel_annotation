/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;
import com.alibaba.csp.sentinel.util.TimeUtil;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * The principle idea comes from Guava. However, the calculation of Guava is
 * rate-based, which means that we need to translate rate to QPS.
 * </p>
 *
 * <p>
 * Requests arriving at the pulse may drag down long idle systems even though it
 * has a much larger handling capability in stable period. It usually happens in
 * scenarios that require extra time for initialization, e.g. DB establishes a connection,
 * connects to a remote service, and so on. That’s why we need “warm up”.
 * </p>
 *
 * <p>
 * Sentinel's "warm-up" implementation is based on the Guava's algorithm.
 * However, Guava’s implementation focuses on adjusting the request interval,
 * which is similar to leaky bucket. Sentinel pays more attention to
 * controlling the count of incoming requests per second without calculating its interval,
 * which resembles token bucket algorithm.
 * </p>
 *
 * <p>
 * The remaining tokens in the bucket is used to measure the system utility.
 * Suppose a system can handle b requests per second. Every second b tokens will
 * be added into the bucket until the bucket is full. And when system processes
 * a request, it takes a token from the bucket. The more tokens left in the
 * bucket, the lower the utilization of the system; when the token in the token
 * bucket is above a certain threshold, we call it in a "saturation" state.
 * </p>
 *
 * <p>
 * Base on Guava’s theory, there is a linear equation we can write this in the
 * form y = m * x + b where y (a.k.a y(x)), or qps(q)), is our expected QPS
 * given a saturated period (e.g. 3 minutes in), m is the rate of change from
 * our cold (minimum) rate to our stable (maximum) rate, x (or q) is the
 * occupied token.
 * </p>
 *
 * @author jialiang.linjl
 */

/**
 * 采用预热令牌桶算法
 */
public class WarmUpController implements TrafficShapingController {

    /**
     * qps限制
     */
    protected double count;

    /**
     * 冷却因子,表示变化快慢,预热期令牌生成间隔/正常期间隔、即稳定期qps为低负载期qps(最冷及以下)的多少倍、默认3
     */
    private int coldFactor;

    /**
     * 剩余令牌进入预热区间阈值、三角区近似面积【coldFactor越大-该值越小-下凹(先慢后快)，coldFactor越小-该值越大-上凸(先快后慢)】
     */
    protected int warningToken = 0;

    /**
     * 预热区间令牌桶容量
     */
    private int maxToken;

    /**
     * 斜率、用于计算剩余token在【报警线,最大容量】线性区间内对应的生成速度(以生成时间间隔衡量)
     */
    protected double slope;

    protected AtomicLong storedTokens = new AtomicLong(0);
    protected AtomicLong lastFilledTime = new AtomicLong(0);

    public WarmUpController(double count, int warmUpPeriodInSec, int coldFactor) {
        construct(count, warmUpPeriodInSec, coldFactor);
    }

    public WarmUpController(double count, int warmUpPeriodInSec) {
        construct(count, warmUpPeriodInSec, 3);
    }

    private void construct(double count, int warmUpPeriodInSec, int coldFactor) {

        if (coldFactor <= 1) {
            throw new IllegalArgumentException("Cold factor should be larger than 1");
        }

        //令count=20、warmUpPeriodInSec=10s、coldFactor=3
        //count=20
        //coldFactor=3
        //warningToken=20*10/(3-1)=100
        //maxToken=100+2*10*20/(1+3)=200
        //slope=(3-1)/20/(200-100)=0.001
        this.count = count;

        this.coldFactor = coldFactor;

        // thresholdPermits = 0.5 * warmupPeriod / stableInterval. 三角面积、0.5 * warmupPeriod * (1/stableInterval=count)=warmUpPeriodInSec * count/2(coldFactor默认3 -1 = 2 ，即默认线性变化)
        // warningToken = 100;
        //0.5*warmupPeriod*(cold_i+stable_i)
        //warmupPeriod/ (cold_i-stable_i)
        warningToken = (int) (warmUpPeriodInSec * count) / (coldFactor - 1);

        //maxPermits = thresholdPermits + 2 * warmupPeriod / (stableInterval + coldInterval)
        // maxToken = 200
        // thresholdPermits + (2 * warmUpPeriodInSec * count / (coldFactor + 1.0))
        //=thresholdPermits + 2 *warmUpPeriodInSec * count /((cold_i+stable_i)/(stable_i))
        //=thresholdPermits + 2 *warmUpPeriodInSec/((cold_i+stable_i)/(stable_i*count))
        //=thresholdPermits + 2 * warmupPeriod / (stableInterval + coldInterval)【warmupPeriod/((stableInterval + coldInterval)/2)=预热时间/时间间隔均值】
        maxToken = warningToken + (int) (2 * warmUpPeriodInSec * count / (coldFactor + 1.0));

        //(coldFactor - 1.0) / count=((coldIntervalMicros-stableIntervalMicros)/(stableIntervalMicros*count=1))=(coldIntervalMicros - stableIntervalMicros)
        //slope=(coldIntervalMicros - stableIntervalMicros) / (maxPermits- thresholdPermits)
        slope = (coldFactor - 1.0) / count / (maxToken - warningToken);
    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        long passQps = (long) node.passQps();

        long previousQps = (long) node.previousPassQps();
        syncToken(previousQps);

        // 开始计算它的斜率
        // 如果进入了警戒线，开始调整他的qps
        long restToken = storedTokens.get();
        if (restToken >= warningToken) {
            //消耗速度过小、表明系统处于冷启动期
            //计算实际剩余token与最小正常消耗速度时剩余token的差值、剩余越多启动期流控限制越小
            long aboveToken = restToken - warningToken;
            // 消耗的速度要比warning快，但是要比慢
            // current interval = restToken(？)*slope + 1/count(稳定区间token生成时间间隔)
            // 当前间隔应等于剩余token*斜率=警戒线上token(剩余token-警戒线)*斜率=线上token*斜率+警戒线生成间隔(稳定期间隔，1.0/count)
            double warningQps = Math.nextUp(1.0 / (aboveToken * slope + 1.0 / count));
            if (passQps + acquireCount <= warningQps) {
                return true;
            }
        } else {
            //正常情况下直接比较当前窗口内qps+申请数是否大于稳定期流控限制
            if (passQps + acquireCount <= count) {
                return true;
            }
        }

        return false;
    }

    protected void syncToken(long passQps) {

        //region 每个滑动窗口期最多仅填充一次token
        long currentTime = TimeUtil.currentTimeMillis();
        currentTime = currentTime - currentTime % 1000;
        long oldLastFillTime = lastFilledTime.get();
        if (currentTime <= oldLastFillTime) {
            return;
        }
        //endregion

        //region 计算token剩余并CAS更新
        long oldValue = storedTokens.get();
        long newValue = coolDownTokens(currentTime, passQps);

        //更新剩余token与填充时间
        if (storedTokens.compareAndSet(oldValue, newValue)) {
            long currentValue = storedTokens.addAndGet(0 - passQps);
            if (currentValue < 0) {
                storedTokens.set(0L);
            }
            lastFilledTime.set(currentTime);
        }
        //endregion
    }

    /**
     * 计算token剩余数
     * 系统处于低负载与高负载(正常负载)情况下均应累积token(不超过最大token容量)
     * 系统处于冷启动负载区token数不变
     */
    private long coolDownTokens(long currentTime, long passQps) {
        long oldValue = storedTokens.get();
        long newValue = oldValue;

        // 添加令牌的判断前提条件:
        // 当令牌的消耗程度远远低于警戒线的时候
        if (oldValue < warningToken) {
            //剩余token小于报警token、已经处于正常消耗速度区间
            //计算正常总剩余 = 上次剩余数 + 过去时间(当前时间-上次更新剩余token时间) * qps阈值(每秒应发放的token)
            newValue = (long) (oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
        } else if (oldValue > warningToken) {
            //剩余token大于报警token、消耗速度处于冷启动区间

            //之前消耗速度小于冷启动最小速度、系统在低负载区间运行
            if (passQps < (int) count / coldFactor) {
                newValue = (long) (oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
            }

            //消耗速度处于冷启动速度区间内token不变
        }
        return Math.min(newValue, maxToken);
    }
}
