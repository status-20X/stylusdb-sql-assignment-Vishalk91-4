const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');
const fs = require('fs').promises;

async function executeSELECTQuery(query) {
    const { fields, table } = parseQuery(query);

    // Check if the file exists
    try {
        await fs.access(`${table}.csv`);
    } catch (err) {
        throw new Error(`File ${table}.csv does not exist`);
    }

    const data = await readCSV(`${table}.csv`);
    
    // Check if the fields exist in the data
    const dataFields = Object.keys(data[0]);
    fields.forEach(field => {
        if (!dataFields.includes(field)) {
            throw new Error(`Field ${field} does not exist in the data`);
        }
    });

    // Filter the fields based on the query
    return data.map(row => {
        const filteredRow = {};
        fields.forEach(field => {
            filteredRow[field] = row[field];
        });
        return filteredRow;
    });
}

module.exports = executeSELECTQuery;