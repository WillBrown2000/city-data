const fs = require('fs');
const csv = require('csv-parser');
const Memcached = require('memcached');
const memcached = new Memcached("localhost:11211")

const { Pool } = require('pg');

const writePool = new Pool({
    user: 'username',
    host: 'localhost',
    database: 'db',
    password: 'secret_password',
    port: 5433,
});

const readPool1 = new Pool({
    user: 'username',
    host: 'localhost',
    database: 'db',
    password: 'secret_password',
    port: 5434,
});

const readPool2 = new Pool({
    user: 'username',
    host: 'localhost',
    database: 'db',
    password: 'secret_password',
    port: 5435,
});

const readPool3 = new Pool({
    user: 'username',
    host: 'localhost',
    database: 'db',
    password: 'secret_password',
    port: 5436,
});

const readPool4 = new Pool({
    user: 'username',
    host: 'localhost',
    database: 'db',
    password: 'secret_password',
    port: 5437,
});

const getPopulation = async (state, city) => {
    const readPools = [readPool1, readPool2, readPool3, readPool4];
    const readPool = readPools[Math.floor(Math.random() * readPools.length)];

    try {
        const res = await writePool.query(
            `SELECT population
             FROM Cities c
             JOIN States s ON c.state_id = s.state_id
             WHERE s.state_name = $1 AND c.city_name = $2`,
            [state, city]
        );
        return res.rows.length > 0 ? res.rows[0].population : null;
    } catch (err) {
        console.error("Error querying population:", err);
        throw err;
    }
};

const upsertPopulation = async (state, city, population) => {
    try {
        // Ensure the state exists, and get its ID
        const stateRes = await writePool.query(
            `INSERT INTO States (state_name)
             VALUES ($1)
             ON CONFLICT (state_name)
             DO UPDATE SET state_name = EXCLUDED.state_name
             RETURNING state_id`,
            [state]
        );

        // Ensure the city exists with the right population, and link to the state
        const cityRes = await writePool.query(
            `INSERT INTO Cities (city_name, state_id, population)
             VALUES ($1, $2, $3)
             ON CONFLICT (city_name, state_id)
             DO UPDATE SET population = EXCLUDED.population
             RETURNING *`,
            [city, stateRes.rows[0].state_id, population]
        );

        return cityRes.rows;
    } catch (err) {
        console.error("Error upserting population:", err);
        throw err;
    }
};


const createTables = async () => {
    await writePool.query(`
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
        .pipe(csv({
            headers: false
        }))
        .on('data', (row) => {
            lineCount++;
            const progress = ((lineCount / totalLines) * 100).toFixed(2);
            process.stdout.write(`Processing: ${lineCount}/${totalLines} (${progress}%) \r`);
            const city = row[0];
            const state = row[1];
            const population = row[2];

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
                    await writePool.query(`
                        INSERT INTO States (state_name)
                        VALUES ($1)
                        ON CONFLICT (state_name)
                        DO NOTHING;
                    `, [state]);

                    await writePool.query(`
                        INSERT INTO Cities (city_name, state_id, population)
                        VALUES ($1, (SELECT state_id FROM States WHERE state_name = $2), $3)
                        ON CONFLICT (city_name, state_id)
                        DO UPDATE SET population = EXCLUDED.population;
                    `, [city, state, population]);

                    const key = `${state.toLowerCase()}:${city.toLowerCase()}`;
                    memcached.set(key, population.toString(), { expires: 600 });
                } catch (err) {
                    console.error(`\nError inserting data [${city}, ${state}, ${population}]:`, err.message);
                }
            }
        });
};

module.exports = {
    writePool,
    getPopulation,
    createTables,
    seedDataFromCSV,
    upsertPopulation
};
