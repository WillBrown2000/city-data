const http = require('http');
const url = require('url');
const { getPopulation, upsertPopulation } = require('./dbQueries');
const { initDB } = require('./initDB.js');
const cluster = require('cluster');
const os = require('os');
const Memcached = require('memcached');

initDB();

const memcached = new Memcached('localhost:11211');

const createServer = () => {
    const server = http.createServer(async (req, res) => {
        const parsedUrl = url.parse(req.url, true);
        const paths = parsedUrl.pathname.split('/').filter(Boolean);

        if (paths.length !== 5 || paths[0] !== 'api' || paths[1] !== 'population' || paths[2] !== 'state') {
            res.writeHead(404);
            return res.end('Not Found');
        }

        let [, , , state, city] = paths;
        state = state.toLowerCase();
        city = city.toLowerCase();

        memcached.stats((err, results) => {
            if (err) {
                console.error(err);
                return;
            }
            console.log('getting stats...')
            const slabs = results[0].slabs;

            for (const slabId in slabs) {
                const slab = slabs[slabId];

                memcached.cachedump(Number(slabId), slab.active_pages, (err, keys) => {
                    if (err) {
                        console.error(err);
                        return;
                    }

                    console.log(`Keys in slab ${slabId}:`, keys.map(key => key.key).join(', '));
                });
            }
        });

        if (req.method === 'GET') {
            try {
                const cacheKey = `${state}:${city}`;
                let population = await memcached.get(cacheKey);
                console.log('population cache', population)

                if (!population) {
                    population = await getPopulation(state, city);
                    memcached.set(cacheKey, population, (err) => {
                        if(err) console.error("Failed to update the cache: ", err);
                    });
                    console.log('population db', population)

                }

                if (population === null) {
                    res.writeHead(400);
                    return res.end(JSON.stringify({ error: `No data found for state: ${state} and city: ${city}` }));
                }

                res.writeHead(200, { 'Content-Type': 'application/json' });
                return res.end(JSON.stringify({ population }));
            } catch (error) {
                res.writeHead(500);
                return res.end(JSON.stringify({ error: 'Internal Server Error' }));
            }
        } else if (req.method === 'PUT') {
            try {
                let data = '';
                req.on('data', chunk => {
                    data += chunk;
                });

                req.on('end', async () => {
                    const population = Number(data);

                    if (isNaN(population) || population < 0) {
                        res.writeHead(400);
                        return res.end(JSON.stringify({ error: 'Invalid population data. Must be a non-negative integer.' }));
                    }

                    if (!state || !city || typeof state !== 'string' || typeof city !== 'string') {
                        res.writeHead(400);
                        return res.end(JSON.stringify({ error: 'Invalid state or city data. Must be non-empty strings.' }));
                    }

                    try {
                        const dbRes = await upsertPopulation(state, city, population);

                        if (dbRes.length > 0) {
                            const cacheKey = `${state}:${city}`;
                            await memcached.add(cacheKey, population);

                            res.writeHead(dbRes[0].population === population ? 201 : 200);
                            return res.end(JSON.stringify({ message: 'Population data updated successfully' }));
                        } else {
                            throw new Error('Data not updated');
                        }
                    } catch (error) {
                        res.writeHead(400);
                        return res.end(JSON.stringify({ error: error.message }));
                    }
                });
            } catch (error) {
                res.writeHead(500);
                return res.end(JSON.stringify({ error: 'Internal Server Error' }));
            }
        } else {
            res.writeHead(405);
            return res.end(JSON.stringify({ error: 'Method Not Allowed' }));
        }
    });

    server.listen(5555, () => {
        console.log(`Worker ${process.pid} started. Server listening on port 5555`);
    });
};

if (cluster.isMaster) {
    console.log(`Master ${process.pid} is running`);

    for (let i = 0; i < os.cpus().length; i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`);
    });
} else {
    createServer();
}
