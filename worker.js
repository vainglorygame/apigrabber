#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    request = require("request-promise"),
    sleep = require("sleep-promise"),
    jsonapi = require("./jsonapi"),
    AdmZip = require("adm-zip");

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
        let payload = JSON.parse(msg.content.toString());
        if (msg.properties.type == "matches")
            await getAPI(payload, "matches");
        if (msg.properties.type == "samples")
            await getAPI(payload, "samples");
        if (msg.properties.type == "sample")
            await getSample(payload);

        console.log("done");
        ch.ack(msg);
    }, { noAck: false });

    // loop over API data pages
    async function getAPI(payload, where) {
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
                gzip: true
            }, failed = false;
            opts.qs = payload.params;
            try {
                console.log("API request: %j", opts.qs);
                let data = await request(opts),
                    datas = jsonapi.parse(data);
                if (where == "matches") {
                    // send match structure
                    await Promise.all(datas
                        .map((match) => ch.sendToQueue("process",
                            new Buffer(JSON.stringify(match)),
                            { persistent: true, type: "match" })
                    ));
                }
                if (where == "samples") {
                    // send to self
                    await Promise.all(datas
                        .map((sample) => ch.sendToQueue("grab",
                            new Buffer(JSON.stringify(sample.attributes.URL)),
                            { persistent: true, type: "sample" })
                    ));
                }
                if (datas.length < 50) exhausted = true;
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
    }

    // download a sample ZIP and send to processor
    async function getSample(url) {
        console.log("downloading sample", url);
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
        console.log("sample processed", url);
    }
})();
