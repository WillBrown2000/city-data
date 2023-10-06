const http = require('http');
const url = require('url');
const { getPopulation, upsertPopulation } = require('./dbQueries');
const { initDB } = require('./initDB.js');
const cluster = require('cluster');
const os = require('os');

initDB();

const createServer = () => {
    const server = http.createServer(async (req, res) => {
        const parsedUrl = url.parse(req.url, true);
        const paths = parsedUrl.pathname.split('/').filter(Boolean);

        if (paths.length !== 5 || paths[0] !== 'api' || paths[1] !== 'population' || paths[2] !== 'state') {
            res.writeHead(404);
            return res.end('Not Found');
        }

        let [, , , state, city] = paths;
        state = state.toLowerCase()
        city = city.toLowerCase()

        if (req.method === 'GET') {
            try {
                const population = await getPopulation(state, city);

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
