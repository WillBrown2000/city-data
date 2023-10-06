import fs from 'fs';
import csv from 'csv-parser';
import pkg from 'pg';
import redis from 'redis';

const redisClient = await redis.createClient({
    host: 'localhost',
    port: 6379
})
    .on('error', err => console.log('Redis Client Error', err))
    .connect();

const { Pool } = pkg;

const dbPool = new Pool({
    user: 'username',
    host: 'localhost',
    database: 'db',
    password: 'secret_password',
    port: 5432,
});

const getPopulation = async (state, city) => {
    const redisKey = `${state}:${city}`;
    try {
        // Try to get the population from Redis first
        const cachedPopulation = await redisClient.get(redisKey);
        if (cachedPopulation !== null) {
            return cachedPopulation;
        }

        // If not in Redis, get from the database
        const res = await dbPool.query(
            `SELECT population
             FROM Cities c
             JOIN States s ON c.state_id = s.state_id
             WHERE s.state_name = $1 AND c.city_name = $2`,
            [state, city]
        );

        const population = res.rows.length > 0 ? res.rows[0].population : null;
        if (population) {
            redisClient.set(redisKey, population);
        }
        return population;
    } catch (err) {
        console.error("Error querying population:", err);
        throw err;
    }
};


const upsertPopulation = async (state, city, population) => {
    try {
        const stateRes = await dbPool.query(
            `INSERT INTO States (state_name)
             VALUES ($1)
             ON CONFLICT (state_name)
             DO UPDATE SET state_name = EXCLUDED.state_name
             RETURNING state_id`,
            [state]
        );

        const cityRes = await dbPool.query(
            `INSERT INTO Cities (city_name, state_id, population)
             VALUES ($1, $2, $3)
             ON CONFLICT (city_name, state_id)
             DO UPDATE SET population = EXCLUDED.population
             RETURNING *, xmax`,
            [city, stateRes.rows[0].state_id, population]
        );

        const wasUpdated = cityRes.rows[0].xmax !== '0';

        redisClient.set(`${state}:${city}`, population);

        return {
            cityRes,
            wasUpdated
        };
    } catch (err) {
        console.error("Error upserting population:", err);
        throw err;
    }
};


const createTables = async () => {
    await dbPool.query(`
        CREATE TABLE IF NOT EXISTS States (
            state_id SERIAL PRIMARY KEY,
            state_name VARCHAR(255) UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Cities (
            city_id SERIAL PRIMARY KEY,
            city_name VARCHAR(255) NOT NULL,
            state_id INT REFERENCES States(state_id),
            population INT NOT NULL,
            UNIQUE(city_name, state_id)
        );
    `);
};

const countLines = async (filePath) => {
    return new Promise((resolve, reject) => {
        let lineCount = 0;
        fs.createReadStream(filePath)
            .on('data', (chunk) => {
                for (let i = 0; i < chunk.length; ++i) {
                    if (chunk[i] == 10) lineCount++;
                }
            })
            .on('end', () => resolve(lineCount))
            .on('error', reject);
    });
};

const seedDataFromCSV = async (csv_file) => {
    const data = [];
    let lineCount = 0;
    const totalLines = await countLines(csv_file);

    fs.createReadStream(csv_file)
        .pipe(csv({ headers: false }))
        .on('data', (row) => {
            lineCount++;
            const progress = ((lineCount / totalLines) * 100).toFixed(2);
            process.stdout.write(`Processing: ${lineCount}/${totalLines} (${progress}%) \r`);
            const city = row[0].toLowerCase();
            const state = row[1].toLowerCase();
            const population = row[2].toLowerCase();

            if (!city || !state || !population || isNaN(population) || parseInt(population) < 0) {
                console.warn(`\nInvalid data detected on line ${lineCount} and skipped: ${JSON.stringify(row)}`);
                return;
            }
            data.push([city, state, population]);
        })
        .on('end', async () => {
            console.log('\nData loading completed. Inserting to database...');
            for (const [city, state, population] of data) {
                try {
                    await dbPool.query(`
                        INSERT INTO States (state_name)
                        VALUES ($1)
                        ON CONFLICT (state_name)
                        DO NOTHING;
                    `, [state]);

                    await dbPool.query(`
                        INSERT INTO Cities (city_name, state_id, population)
                        VALUES ($1, (SELECT state_id FROM States WHERE state_name = $2), $3)
                        ON CONFLICT (city_name, state_id)
                        DO UPDATE SET population = EXCLUDED.population;
                    `, [city, state, population]);

                    const key = `${state.toLowerCase()}:${city.toLowerCase()}`;
                    await redisClient.set(key, population);
                } catch (err) {
                    console.error(`\nError inserting data [${city}, ${state}, ${population}]:`, err.message);
                }
            }

            await redisClient.quit();
        });
};
export {
    dbPool,
    getPopulation,
    createTables,
    seedDataFromCSV,
    upsertPopulation
};
