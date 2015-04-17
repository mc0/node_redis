var redis = require("./"),
    PORT = process.argv[2] || 6379,
    HOST = process.argv[3] || '127.0.0.1',
    c;
//redis.debug_mode = true

c = redis.createClient(PORT, HOST);
c.unref();
c.info(function (err, reply) {
    if (err) {
        process.exit(-1);
    }
    if (!reply.length) {
        process.exit(-1);
    }

    process.stdout.write(reply.length.toString());
});
