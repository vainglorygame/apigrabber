#!/usr/bin/node
/* jshint esnext:true */
"use strict";

const amqp = require("amqplib"),
    winston = require("winston"),
    request = require("request-promise"),
    sleep = require("sleep-promise"),
    jsonapi = require("./jsonapi"),
    AdmZip = require("adm-zip");

const MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    GRABBERS = parseInt(process.env.GRABBERS) || 4;
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

const logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            timestamp: () => Date.now(),
            formatter: (options) => winston.config.colorize(options.level,
`${new Date(options.timestamp()).toISOString()} ${options.level.toUpperCase()} ${(options.message? options.message:"")} ${(options.meta && Object.keys(options.meta).length? JSON.stringify(options.meta):"")}`)
        })
    ]
});

(async () => {
    let rabbit, ch;

    while (true) {
        try {
            rabbit = await amqp.connect(RABBITMQ_URI);
            ch = await rabbit.createChannel();
            await ch.assertQueue("grab", {durable: true});
            await ch.assertQueue("grab_sample", {durable: true});
            await ch.assertQueue("process", {durable: true});
            break;
        } catch (err) {
            logger.error("error connecting", err);
            await sleep(5000);
        }
    }

    await ch.prefetch(GRABBERS);
    ch.consume("grab", async (msg) => {
        let payload = JSON.parse(msg.content.toString()),
            notify = msg.properties.headers.notify;  // where to send progress report
        if (msg.properties.type == "matches")
            await getAPI(payload, "matches", notify);
        if (msg.properties.type == "samples")
            await getAPI(payload, "samples");

        logger.info("done", payload);
        ch.ack(msg);
    }, { noAck: false });

    ch.consume("grab_sample", async (msg) => {
        let payload = JSON.parse(msg.content.toString());
        if (msg.properties.type == "sample")
            await getSample(payload);

        logger.info("done", payload);
        ch.ack(msg);
    }, { noAck: false });

    // loop over API data pages, notify of progress
    async function getAPI(payload, where, notify="global") {
        let exhausted = false;
        payload.params["page[limit]"] = payload.params["page[limit]"] || 50;
        payload.params["page[offset]"] = payload.params["page[offset]"] || 0;

        while (!exhausted) {
            let opts = {
                uri: "https://api.dc01.gamelockerapp.com/shards/" + payload.region + "/" + where,
                headers: {
                    "X-Title-ID": "semc-vainglory",
                    "Authorization": MADGLORY_TOKEN
                },
                json: true,
                gzip: true,
                time: true,
                forever: true,
                strictSSL: true,
                resolveWithFullResponse: true
            }, failed = false, response;
            opts.qs = payload.params;
            try {
                logger.info("API request", { uri: opts.uri, qs: opts.qs });
                response = await request(opts);
                let data = jsonapi.parse(response.body);
                if (where == "matches") {
                    // send match structure
                    await Promise.all(data
                        .map((match) => ch.sendToQueue("process",
                            new Buffer(JSON.stringify(match)),
                            { persistent: true, type: "match" })
                    ));
                }
                if (where == "samples") {
                    // send to self
                    await Promise.all(data
                        .map((sample) => ch.sendToQueue("grab_sample",
                            new Buffer(JSON.stringify(sample.attributes.URL)),
                            { persistent: true, type: "sample" })
                    ));
                }
                // tell web about progress
                await ch.publish("amq.topic", notify,
                    new Buffer("grab_success"));
            } catch (err) {
                response = err.response;
                if (err.statusCode == 429) {
                    await sleep(1000);
                } else if (err.statusCode == 404) {
                    exhausted = true;
                } else {
                    logger.error("API error",
                        { uri: err.options.uri, qs: err.options.qs, error: err.response.body });
                    exhausted = true;
                }
                failed = true;
            } finally {
                logger.info("API response: status %s, connection start %s, connection end %s, ratelimit remaining: %s",
                    response.statusCode, response.timings.connect, response.timings.end, response.headers["x-ratelimit-remaining"]);
            }

            // next page
            if (!failed)
                payload.params["page[offset]"] += payload.params["page[limit]"]
        }
        await ch.publish("amq.topic", notify,
            new Buffer("grab_done"));
    }

    // download a sample ZIP and send to processor
    async function getSample(url) {
        logger.info("downloading sample", url);
        let zipdata = await request({
            uri: url,
            encoding: null
        }),
            zip = new AdmZip(zipdata);
        await Promise.all(zip.getEntries().map(async (entry) => {
            if (entry.isDirectory) return;
            let match = jsonapi.parse(JSON.parse(entry.getData().toString("utf8")));
            await ch.sendToQueue("process",
                new Buffer(JSON.stringify(match)),
                { persistent: true, type: "match" })
        }));
        logger.info("sample processed", url);
    }
})();

process.on("unhandledRejection", err => {
    logger.error("Uncaught Promise Error:", err.stack);
});
