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
    

    const redis = new Redis('redis://localhost:6379/3');
    redis.on("error", (c) => console.log (`error ${c}`));
    redis.on("ready", (c) => {
        console.log ('redis ready');
        const REDIS_CHANNEL = 'clickpath';
        let promiseArray = [];
        for (var l = 0; l < (process.env.MESSAGES || 1000); l++) {
            //let publish_promise = redis.publish(REDIS_CHANNEL,  `{"itteration": ${l}, "workerid": ${cluster.worker.id}, "message": "hello from ${cluster.worker.id}:${l}"}`);
            let publish_promise = redis.lpush(REDIS_CHANNEL,  `{"itteration": ${l}, "workerid": ${cluster.worker.id}, "message": "hello from ${cluster.worker.id}:${l}"}`);
            if (process.env.DELAY) {
                promiseArray.push (() => {return new Promise((a,b) => setTimeout (() => {console.log (`send from ${cluster.worker.id}/${l}`); publish_promise.then(() => a());}, process.env.DELAY))});
            } else {
                promiseArray.push (publish_promise);
            }
        }
        promiseArray.reduce((p, fn) => p.then(fn, (err) => console.log (err)), Promise.resolve()).then((res) => {
            console.log (`finished ${cluster.worker.id}`);
            //redis.quit();
            process.exit(0);
        })
    });
}