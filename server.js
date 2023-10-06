import http from 'http';
import url from 'url';
import cluster from 'cluster';
import os from 'os';
import { initDB } from './initDB.js';
import { getPopulation, upsertPopulation } from './controllers.js';



initDB();

const createServer = () => {
    const server = http.createServer(async (req, res) => {
        const parsedUrl = url.parse(req.url, true);
        const paths = parsedUrl.pathname.split('/').filter(Boolean);

        if (paths.length !== 6 || paths[0] !== 'api' || paths[1] !== 'population' || paths[2] !== 'state' || paths[4] !== 'city') {
            res.writeHead(404);
            return res.end('Not Found');
        }

        let [, , , state, , city] = paths;
        state = state.toLowerCase();
        city = city.toLowerCase();

        if (req.method === 'GET') {
            try {
                console.log('state, city: ', state, city)
                const population = await getPopulation(state, city);
                console.log('pop: ', population)
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

                try {

                    const { wasUpdated } = await upsertPopulation(state, city, population);

                    res.writeHead(wasUpdated ? 200 : 201);
                    return res.end();
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

if (cluster.isPrimary) {
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

process.on('exit', () => {
    redisClient.quit();
});
