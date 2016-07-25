const amqp10 = require('amqp10'),
    AMQPClient = amqp10.Client,
    Policy = amqp10.Policy,
    CONSUMER_GROUP = '$Default',
    PARTITIONS = 4

const ENTITY_PATH = process.env.ENTITY_PATH // enventhub namespace
const AMQP_URL = process.env.AMQP_URL

if (!ENTITY_PATH || !AMQP_URL) {
    console.warn ("ENTITY_PATH and AMQP_URL need to be set");
    process.exit(1);
}
const RXFILTER = {
            attach: { source: { filter: {
              'apache.org:selector-filter:string': amqp10.translator(
                ['described', ['symbol', 'apache.org:selector-filter:string'], ['string', `amqp.annotation.x-opt-enqueuedtimeutc > '${new Date().getTime()}'`]])
            } } }
          }

var client = new AMQPClient(Policy.EventHub); // Uses PolicyBase default policy
var received = 0, receivederr = 0
client.connect(AMQP_URL).then(() => {

    for (let i = 0; i < PARTITIONS; i++) {
        client.createReceiver(`${ENTITY_PATH}/ConsumerGroups/${CONSUMER_GROUP}/Partitions/${i}`, RXFILTER).then((receiver) => { 
            receiver.on('message', (msg) => {
                received++
                //console.warn(`==> RX ${JSON.stringify(msg)}`)
            })
            receiver.on('errorReceived', (rx_err) => {
                receivederr++
                console.warn(`==> RX ERROR: ${JSON.stringify(rx_err)}`)
            })
        })
    }
})

var r = 0, re = 0
setInterval(() => {
    if (received !== r || receivederr !== re) {
        r = received; re = receivederr;
        console.log (`received : ${received}, errs: ${receivederr}`)
    }
}, 2000);
