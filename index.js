
const eventHub = require('azure-event-hubs').Client
const Redis = require('ioredis')

const redis = new Redis('redis://localhost:6379/3', {
enableOfflineQueue: true,
reconnectOnError: function (err) {
    console.log(`redis err ${err}`)
},
retryStrategy: function (times) {
    var delay = Math.min(times * 2, 2000)
    return delay
}
});
redis.on("ready", (c) => console.log ('redis ready'))
redis.on("error", (c) => console.log (`redis error: ${JSON.stringify(c)}`))


const connectionString = 'Endpoint=sb://mapusageingest-ns.servicebus.windows.net/;SharedAccessKeyName=RedisSend;SharedAccessKey=PHyXTDY09cfC3pIpCLKc5cHX8rB6tGPrROa4u+vG6dc=;EntityPath=mapusageingest'
const eventHubPath = '/clickpath'
const partitionId = '0'

// -------------------- Connect to Azure Event Hub
var client = eventHub.fromConnectionString(connectionString, eventHubPath);
client.createSender(partitionId).then((sender) => {
    sender.on('errorReceived',  (err) => { console.log(err); })

    console.log (`Created EventHub Sender`)

    const REDIS_CHANNEL = 'clickpath'

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

    // pop loop
    const max_send_waiting = process.env.CONCURRENT || 20
    let sent = 0 , confirmed = 0, errors = 0,
        subscribe = () => {
            redis.brpop(REDIS_CHANNEL, 0).then ((message) => {
                sender.send(message).then ((res) => {
                    // promises are resolved when a disposition frame is received from the remote link for the sent message, at this point the message is considered "settled". 
                    confirmed++; 
            //      if (send_waiting == (max_send_waiting - 1)) 
            //         subscribe();
                    //console.log (`sent: ${JSON.stringify(res)}`);
                }, (err) => {
                    errors++;
            //      if (send_waiting == (max_send_waiting - 1)) 
            //         subscribe();
                    console.log (`send error: ${JSON.stringify(err)}`)
                });

                sent++
                //  if (send_waiting < max_send_waiting) 
                    subscribe()
            });
        };

    // print metrics if they change
    var l = -1, s = 0, c = 0, e = 0
    setInterval(() => {
        //redis.llen(REDIS_CHANNEL).then( (llen) => {
            if (sent !== s || confirmed !== c || errors !== e) {
                s = sent; c = confirmed; e = errors
                console.log (`errors: ${errors}, confirmed: ${confirmed}, inprogress: ${sent - confirmed}`)
            }
        //}, (err) => console.log (`cannot get list length ${err}`))
    }, 2000);

    subscribe();

    // close down connections on ctrl-c
    process.on ('SIGINT', (code) => {
        try {
            sender.close().then((res) => console.log ('closed sender'));
            console.log ('closed connetions');
            process.exit(0);
        } catch (e) {

        }
    })
});