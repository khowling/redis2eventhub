
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
var sent = 0, confirmed = 0, errors = 0
client.connect(AMQP_URL).then(() => {
    client.createSender(ENTITY_PATH).then ((sender) => {
        sender.on('errorReceived', function (tx_err) { console.warn('===> TX ERROR: ', tx_err); });
        //let test_message = { DataString: 'From Node', DataValue: "hello from index2" }; 
        let options = { annotations: { 'x-opt-partition-key': 'pk' + 'nodeapp' } }; 
        let subscribe = () => {
                redis.brpop(REDIS_CHANNEL, 0).then ((message) => {
                    try {
                        let msg = JSON.parse(message[1]);
                        sender.send(msg).then ((res) => {
                            // promises are resolved when a disposition frame is received from the remote link for the sent message, at this point the message is considered "settled". 
                            confirmed++; 
                        }, (err) => {
                            errors++;
                            console.log (`===> TX ERROR: ${JSON.stringify(err)}`)
                        });
                        sent++
                        subscribe()
                    } catch (e) {
                        console.error (`failed to process redis message ${e}`);
                    }
                });
            };
        // start the blocking loop
        subscribe();

        // close down connections on ctrl-c
        process.on ('SIGINT', (code) => {
            try {
                sender.detach().then((res) => console.log ('closed sender'))
                console.log ('closed connetions')
            } catch (e) {
                console.log (`cannot detach sender: ${e}`)
            }
            process.exit(0)
        })
    }, (err) => console.log (`failed to create sneder : ${err}`))
}, (err) => console.log (`failed to connect to eventhub : ${err}`))


// stdout logging
var s = 0, c = 0, e = 0 // to detect changes in metrics, only log if changed
setInterval(() => {
    if (sent !== s || confirmed !== c || errors !== e) {
        s = sent; c = confirmed; e = errors;
        console.log (`inprogress: ${sent - confirmed}, confirmed: ${confirmed}, errors: ${errors})`)
    }
}, 2000);

