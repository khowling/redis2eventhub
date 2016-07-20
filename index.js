
const eventHub = require('azure-event-hubs').Client;
const Redis = require('ioredis');
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
    console.log ('created EventHub Sender');

    const REDIS_CHANNEL = 'clickpath';
    redis.subscribe(REDIS_CHANNEL,  (err, count) => {
        if (err) {
            process.exitCode = 1;
            throw new Error(`cannot subscribe to redis channel ${REDIS_CHANNEL}`);
        } 
        console.log (`subscribed to channel ${REDIS_CHANNEL} (#=${count})`);
    });

    redis.on('message', function (channel, message) {
        console.log(`Receive message ${message} from channel ${channel}`);
        sender.send(message).then ((res) => {
            console.log (`sent: ${JSON.stringify(res)}`);
        });
    });

    process.on ("exit", (code) => {
        sender.close().then((res) => console.log ('closed sender'));
        client.close().then((res) => console.log ('closed client'));
    })
});






