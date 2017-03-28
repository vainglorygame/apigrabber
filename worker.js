#!/usr/bin/node
/* jshint esnext:true */

var amqp = require("amqplib"),
    request = require("request-promise"),
    sleep = require("sleep-promise");

var MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost";
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

(async () => {
    var rabbit = await amqp.connect(RABBITMQ_URI),
        ch = await rabbit.createChannel();

    await ch.assertQueue("grab", {durable: true});
    await ch.assertQueue("process", {durable: true});
    await ch.prefetch(1);

    ch.consume("grab", async (msg) => {
        let exhausted = false;
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
            };
            opts.qs = payload.params;
            try {
                console.log("API request: %j", opts.qs);
                res = await request(opts);
                console.log("got a few matches");
                await ch.sendToQueue("process", new Buffer(JSON.stringify(res)), { persistent: true });
            } catch (err) {
                if (err.statusCode == 429) {
                    await sleep(1000);
                } else if (err.statusCode == 404) {
                    // TODO stop early if len(matches) < pagelen
                    exhausted = true;
                } else {
                    console.error(err);
                    exhausted = true;
                }
                console.log(err.statusCode);
            }

            // next page
            payload.params["page[offset]"] += payload.params["page[limit]"]
        }

        console.log("done");
        ch.ack(msg);
    }, { noAck: false });
})();
