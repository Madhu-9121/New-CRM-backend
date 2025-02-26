'use strict';

const path = require("path");
const fs = require('fs');

/**
 * retrieve the config from the JSON config file
 * @param {String} relativeFilePath relative path to the config file
 * @param {Boolean} optional is config optional (return blank string if not present)
 */
const getConfigJSON = function (relativeFilePath, optional) {
    const config = {};
    const configPath = path.resolve(__dirname, relativeFilePath);
    const doesConfigFileExists = fs.existsSync(configPath);
    if (doesConfigFileExists || !optional) {
        const configString = fs.readFileSync(configPath, {
            encoding: "utf8"
        });
        Object.assign(config, JSON.parse(configString));
    }
    return config;
};

module.exports = {
    getConfigJSON
}