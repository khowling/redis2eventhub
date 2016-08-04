
const amqp10 = require('amqp10'),
    AMQPClient = amqp10.Client,
    Policy = amqp10.Policy,
    Redis = require('ioredis'),
    redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379/3', {
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

const REDIS_CHANNEL = process.env.REDIS_CHANNEL || 'clickpath' // redis queue
const ENTITY_PATH = process.env.ENTITY_PATH // enventhub namespace
const AMQP_URL = process.env.AMQP_URL

if (!ENTITY_PATH || !AMQP_URL) {
    console.warn ("ENTITY_PATH and AMQP_URL need to be set");
    process.exit(1);
}

var client = new AMQPClient(Policy.EventHub) // Uses PolicyBase default policy
var sent = 0, settled = 0, errors = 0
// create AMQP Connection to the Azure Service Bus Container namespace (expensive, setup TCP/TLS etc)
client.connect(AMQP_URL).then(() => {
    // this nodejs app 'Node', opens up a 'link' by calling 'attach' between itself (as sender) & the EventHub 'Node' (as receiver)
    client.createSender(ENTITY_PATH).then ((sender) => {
        sender.on('errorReceived', (tx_err) => {
            console.warn('===> TX ERROR: ', tx_err)
            console.log(`===> TX messages sending: ${sent - (settled+errors)}, settled: ${settled}, failed: ${errors} (linkCredit: ${linkCredit})`)
        });
        redis.psubscribe("*",  (err, count) => {
            if (err) {
                process.exitCode = 1;
                throw new Error(`cannot subscribe to redis channel ${REDIS_CHANNEL}`);
            } 
            console.log (`subscribed to channel ${REDIS_CHANNEL} (#=${count})`);
        });

        redis.on('pmessage', (pattern, channel, message) => {
            try {
                let msg = JSON.parse(message);
                sender.send(msg).then ((res) => {
                    // promises are resolved when a disposition frame is received from the remote link for the sent message, at this point the message is considered "settled". 
                    settled++; 
                }, (err) => {
                    errors++;
                    console.log (`===> TX Error: ${JSON.stringify(err)}`)
                });
                sent++;
            } catch (e) {
                errors++;
                console.error (`failed to JSON parse redis message ${e}`)
            }
        })

        // Manage message flow control
        sender.on('creditChange', (flow) => {
            //console.log(`===> TX flow frame: linkCredit: ${flow.linkCredit}, delivery: ${flow.deliveryCount}. messages sending: ${sent - (settled+errors)}, settled: ${settled}, failed: ${errors} (linkCredit: ${linkCredit})`)
        })

        // close down connections on ctrl-c
        process.on ('SIGINT', (code) => {
            try {
                sender.detach().then((res) => console.log ('detached sender'))
                console.log ('closed connetions')
            } catch (e) {
                console.log (`cannot detach sender: ${e}`)
            }
            setInterval (() => process.exit(0), 500);
        })
    }, (err) => console.log (`failed to create sneder : ${err}`))
}, (err) => console.log (`failed to connect to eventhub : ${err}`))


// stdout logging
var s = 0, c = 0, e = 0 // to detect changes in metrics, only log if changed
setInterval(() => {
    if (sent !== s || settled !== c || errors !== e) {
        s = sent; c = settled; e = errors;
        console.log (`messages sending: ${sent - (settled+errors)}, settled: ${settled}, failed: ${errors}`)
    }
}, 2000);

