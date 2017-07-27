#!/usr/bin/node
/* jshint esnext:true */
"use strict";

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    request = require("request-promise"),
    api = require("../orm/api");

const MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    QUEUE = process.env.QUEUE || "grab",
    PROCESS_QUEUE = process.env.PROCESS_QUEUE || "process",
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN;
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

const logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            timestamp: true,
            colorize: true
        })
    ]
});

// loggly integration
if (LOGGLY_TOKEN)
    logger.add(winston.transports.Loggly, {
        inputToken: LOGGLY_TOKEN,
        subdomain: "kvahuja",
        tags: ["backend", "apigrabber", QUEUE],
        json: true
    });

amqp.connect(RABBITMQ_URI).then(async (rabbit) => {
    process.once("SIGINT", rabbit.close.bind(rabbit));

    const ch = await rabbit.createChannel();
    await ch.assertQueue(QUEUE, { durable: true });
    await ch.assertQueue(QUEUE + "_failed", { durable: true });
    await ch.assertQueue(PROCESS_QUEUE, { durable: true });
    await ch.prefetch(1);  // 1 worker = 1 grab job

    // main queue
    ch.consume(QUEUE, async (msg) => {
        try {
            let payload = JSON.parse(msg.content.toString()),
                notify = msg.properties.headers.notify; // where to send progress report
            await getAPI(payload, notify);
            logger.info("done", payload);
        } catch (err) {
            // log, move to error queue and NACK on *any* error
            logger.error(err);
            await ch.sendToQueue(QUEUE + "_failed", msg.content, {
                persistent: true,
                headers: msg.properties.headers
            });
            await ch.nack(msg, false, false);
            return;
        }
        await ch.ack(msg);
    }, { noAck: false });

    // loop over API data objects
    async function getAPI(payload, notify) {
        const data = await Promise.each(await api.requests("matches",
            payload.region, payload.params, logger),
            async (data, idx, len) => {
                await ch.sendToQueue(PROCESS_QUEUE,
                    new Buffer(JSON.stringify(data)), {
                        persistent: true, type: "match",
                        headers: { notify }
                    });
                    // forward "notify" for the last match on the last page
                if (notify) await ch.publish("amq.topic", notify,
                    new Buffer("match_pending"));
                return data;
            }
        );
        if (data.length == 0 && notify)
            await ch.publish("amq.topic", notify,
                new Buffer("matches_none"));
    }
});

process.on("unhandledRejection", (err) => {
    logger.error("Uncaught Promise Error:", err.stack);
    process.exit(1);  // fail hard and die
});
