
const eventHub = require('azure-event-hubs').Client;
const Redis = require('ioredis');

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
if (cluster.isMaster) {
    // Fork workers.
    for (var i = 0; i < (process.env.WORKERS || numCPUs); i++) {
        cluster.fork();
    }
    cluster.on('exit', (worker, code, signal) => {
        console.log(`worker ${worker.process.pid} exit`);
    });
} else {
    console.log(`worker started #${cluster.worker.id}`);

    const redis = new Redis('redis://localhost:6379/3', {
    enableOfflineQueue: true,
    reconnectOnError: function (err) {
        console.log(`redis err ${err}`);
    },
    retryStrategy: function (times) {
        var delay = Math.min(times * 2, 2000);
        return delay;
    }
    });
    redis.on("ready", (c) => console.log ('redis ready'));
    redis.on("error", (c) => console.log (`error ${c}`));


    const connectionString = 'Endpoint=sb://mapusageingest-ns.servicebus.windows.net/;SharedAccessKeyName=RedisSend;SharedAccessKey=PHyXTDY09cfC3pIpCLKc5cHX8rB6tGPrROa4u+vG6dc=;EntityPath=mapusageingest';
    const eventHubPath = '/clickpath';
    const partitionId = '0';

    // -------------------- Connect to Azure Event Hub
    var client = eventHub.fromConnectionString(connectionString, eventHubPath);
    client.createSender(partitionId).then((sender) => {
        sender.on('errorReceived',  (err) => { console.log(err); });

        console.log (`#${cluster.worker.id} Created EventHub Sender`);

        const REDIS_CHANNEL = 'clickpath';

        /* -- uncomment if using pub/sub
        redis.subscribe(REDIS_CHANNEL,  (err, count) => {
            if (err) {
                process.exitCode = 1;
                throw new Error(`cannot subscribe to redis channel ${REDIS_CHANNEL}`);
            } 
            console.log (`subscribed to channel ${REDIS_CHANNEL} (#=${count})`);
        });
        redis.on('message', (channel, message) => {
        */
        const max_send_waiting = process.env.CONCURRENT || 50;
        let send_waiting = 0 , totalsent = 0;
            subscribe = () => {
                redis.brpop(REDIS_CHANNEL, 0).then ((message) => {
                    sender.send(message).then ((res) => {
                        send_waiting--; totalsent++;
                        if (send_waiting == (max_send_waiting - 1)) subscribe();
                        //console.log (`sent: ${JSON.stringify(res)}`);
                    }, (err) => console.log (`error: ${err}`));

                    send_waiting++;
                    if (send_waiting < max_send_waiting) subscribe();
                });
            };
        setInterval(() => console.log (`#${cluster.worker.id} totalsent : ${totalsent}, inprogress: ${send_waiting}`), 1000);
        subscribe();

        process.on ('SIGINT', (code) => {
            try {
                sender.close().then((res) => console.log ('closed sender'));
                console.log ('closed connetions');
            } catch (e) {

            }
        })
    });
}