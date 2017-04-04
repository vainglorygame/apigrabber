#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    request = require("request-promise"),
    sleep = require("sleep-promise"),
    jsonapi = require("./jsonapi");

var MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost";
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

(async () => {
    let rabbit, ch;

    while (true) {
        try {
            rabbit = await amqp.connect(RABBITMQ_URI);
            ch = await rabbit.createChannel();
            await ch.assertQueue("grab", {durable: true});
            await ch.assertQueue("process", {durable: true});
            break;
        } catch (err) {
            console.error(err);
            await sleep(5000);
        }
    }

    await ch.prefetch(1);
    ch.consume("grab", async (msg) => {
        let exhausted = false,
            payload = JSON.parse(msg.content);
        payload.params["page[limit]"] = payload.params["page[limit]"] || 50;
        payload.params["page[offset]"] = payload.params["page[offset]"] || 0;

        while (!exhausted) {
            let opts = {
                uri: "https://api.dc01.gamelockerapp.com/shards/" + payload.region + "/matches",
                headers: {
                    "X-Title-ID": "semc-vainglory",
                    "Authorization": MADGLORY_TOKEN
                },
                json: true,
                gzip: true
            }, failed = false;
            opts.qs = payload.params;
            try {
                console.log("API request: %j", opts.qs);
                let data = await request(opts),
                    matches = jsonapi.parse(data);
                // send match structure
                await Promise.all(matches
                    .map((match) => ch.sendToQueue("process",
                        new Buffer(JSON.stringify(match)),
                        { persistent: true, type: "match" })
                ));
                if (matches.length < 50) exhausted = true;
            } catch (err) {
                if (err.statusCode == 429) {
                    await sleep(1000);
                } else if (err.statusCode == 404) {
                    exhausted = true;
                } else {
                    console.error(err);
                    exhausted = true;
                }
                console.log(err.statusCode);
                failed = true;
            }

            // next page
            if (!failed)
                payload.params["page[offset]"] += payload.params["page[limit]"]
        }

        console.log("done");
        ch.ack(msg);
    }, { noAck: false });
})();
