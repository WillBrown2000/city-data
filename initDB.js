const { createTables, seedDataFromCSV } = require('./dbQueries');

const initDB = async () => {
    try {
        console.log('creating tables...')
        await createTables();
        console.log('tables created successfully...')
        console.log('seeding city data...')
        await seedDataFromCSV('city-data.csv');
        console.log('city data seeded successfully...')
        console.log('database initialization complete -> All your base are belong to us.')
    } catch (err) {
        console.error(err);
    }
};

module.exports = {
    initDB
};