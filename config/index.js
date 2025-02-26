'use strict';

const { getConfigJSON } = require('./retrieve_config');
const rawConfig = getConfigJSON('./app/config.json', true);
const path = require('path')
    , hfc = require('fabric-client');

const config = {
    otp_expire_time: 300,
    invoice_discounting_link_expire_time: 43200, //12 hrs
    ...rawConfig
};

// Fabric network
const fabricNetworkConfigFile = 'network-config.yaml';
const setFabricNetworkConfig = function (configParam) {
    hfc.setConfigSetting('network-connection-profile-path', path.join(__dirname, '../artifacts', fabricNetworkConfigFile));
    hfc.setConfigSetting('admin-connection-profile-path', path.join(__dirname, '../artifacts', 'admin.yaml'));
    hfc.setConfigSetting('member-connection-profile-path', path.join(__dirname, '../artifacts', 'member.yaml'));
    for (const key in configParam) {
        if (Object.prototype.hasOwnProperty.call(configParam, key)) {
            hfc.setConfigSetting(key, configParam[key]);
        }
    }
}

setFabricNetworkConfig(config.hfc);

module.exports = config;