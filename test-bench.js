const Redis = require('ioredis');

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const testLongStr = 'Around 250,000 people making their way to the Port of Dover were stuck in traffic for 15 hours yesterday (centre) - with many motorists forced to sleep in their vehicles overnight. The reason for the chaos was heightened French security on this side of the channel in the wake of the terror attack in Nice last week. Port authorities said French border control booths at Dover had been "seriously understaffed" overnight, with just three of the seven passport control booths open. At the height of the mayhem, just one member of the French border force was checking passengers" passports on hundreds of coaches - taking 40 minutes to check each coach. Rob Jackson (top right) left Golcar in West Yorkshire with his partner and two children at 8am Saturday morning and were still stuck in traffic some 15 hours later. Social media users on Twitter have questioned whether the turmoil in Kent is a "2Brexit hate crime" while others have suggested the French authorities are giving Brits a "hard time" by "making it awkward" following the referendum. One user suggested the hold another referendum vote on the M20 (inset) near Dover - just 20 miles from Calais in France. '

if (cluster.isMaster) {
    // Fork workers.
    //for (var i = 0; i < (process.env.WORKERS || numCPUs); i++) {
        cluster.fork();
    //}
    cluster.on('exit', (worker, code, signal) => {
        console.log(`worker ${worker.process.pid} exit`);
    });
} else {
    console.log(`worker started #${cluster.worker.id}`);
    

    const redis = new Redis('redis://localhost:6379/3');
    redis.on("error", (c) => console.log (`redis error: ${JSON.stringify(c)}`));
    redis.on("ready", (c) => {
        console.log ('redis ready');
        const REDIS_CHANNEL = 'clickpath';
        let promiseArray = [];
        for (var l = 0; l < (process.env.MESSAGES || 1); l++) {
            //let publish_promise = redis.publish(REDIS_CHANNEL,  `{"itteration": ${l}, "workerid": ${cluster.worker.id}, "message": "hello from ${cluster.worker.id}:${l}"}`);
            let publish_promise = redis.lpush(REDIS_CHANNEL,  `{"body": ${testLongStr}, itteration": ${l}, "workerid": ${cluster.worker.id}, "message": "hello from ${cluster.worker.id}:${l}"}`);
            if (process.env.DELAY) {
                promiseArray.push (() => {return new Promise((a,b) => setTimeout (() => {console.log (`send from ${cluster.worker.id}/${l}`); publish_promise.then(() => a());}, process.env.DELAY))});
            } else {
                promiseArray.push (publish_promise);
            }
        }
        promiseArray.reduce((p, fn) => p.then(fn, (err) => console.log (`send error : ${JSON.stringify(err)}`)), Promise.resolve()).then((res) => {
            console.log (`finished ${cluster.worker.id}`);
            //redis.quit();
            process.exit(0);
        })
    });
}