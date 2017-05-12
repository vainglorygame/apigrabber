#!/usr/bin/node
/* jshint esnext:true */
"use strict";

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    request = require("request-promise"),
    sleep = require("sleep-promise"),
    api = require("../orm/api");

const MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    QUEUE = process.env.QUEUE || "grab",
    PROCESS_QUEUE = process.env.PROCESS_QUEUE || "process",
    SAMPLE_QUEUE = process.env.SAMPLE_QUEUE || "sample",
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    GRABBERS = parseInt(process.env.GRABBERS) || 4;
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

(async () => {
    let rabbit, ch;

    while (true) {
        try {
            rabbit = await amqp.connect(RABBITMQ_URI);
            ch = await rabbit.createChannel();
            await ch.assertQueue(QUEUE, {durable: true});
            await ch.assertQueue(PROCESS_QUEUE, {durable: true});
            break;
        } catch (err) {
            logger.error("error connecting", err);
            await sleep(5000);
        }
    }

    await ch.prefetch(GRABBERS);
    // main queue
    ch.consume(QUEUE, async (msg) => {
        let payload = JSON.parse(msg.content.toString()),
            notify = msg.properties.headers.notify;  // where to send progress report
        if (msg.properties.type == "matches")
            await getAPI(payload, "matches", notify);
        if (msg.properties.type == "samples")
            await getAPI(payload, "samples");

        logger.info("done", payload);
        ch.ack(msg);
    }, { noAck: false });

    // loop over API data objects
    async function getAPI(payload, where, notify="global") {
        await Promise.each(await api.requests(where,
            payload.region, payload.params, logger),
            async (data, idx, len) => {
                if (where == "matches") // send match structure
                    await ch.sendToQueue(PROCESS_QUEUE,
                        new Buffer(JSON.stringify(data)), {
                            persistent: true, type: "match",
                            headers: idx == len - 1? { notify: notify } : {}
                        });
                        // forward "notify" for the last match on the last page
                if (where == "samples")
                    // forward to sampler
                    await ch.sendToQueue(SAMPLE_QUEUE,
                        new Buffer(JSON.stringify(data.attributes.URL)),
                        { persistent: true, type: "sample" });
            }
        );
    }
})();

process.on("unhandledRejection", err => {
    logger.error("Uncaught Promise Error:", err.stack);
});
