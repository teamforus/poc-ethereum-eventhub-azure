/*jshint esversion: 6 */

const partitionId = "1";

const AzureEventHubs = require('azure-event-hubs');
const EventHubClient = AzureEventHubs.EventHubClient;

const bodyKey = 'body';

const configSelector = 'eventHub';
const configEventHubNameSelector = 'eventHubName';
const configConnectionStringSelector = 'connectionString';

const eventList = require('./../event-list');
const eventListData = eventList.Data;
const eventNameKey = eventListData.EVENT_NAME;
const eventDataKey = eventListData.EVENT_DATA;

const fileSystem = require('fs');

var client = null;
var clientName = "undefined";
var lastEvent = -1;
var lastEventFile = './.last';
var receiver = null;
var version = {};

module.exports = 
{
    'configure': (config, packageInfo) => {
        if (configSelector in config) {
            const eventHubConfig = config[configSelector];
            if (!configConnectionStringSelector in eventHubConfig) throw 'No connectionString in EventHub config';
            if (!configEventHubNameSelector in eventHubConfig) throw 'No eventHubName in EventHub config';
            const eventHubName = eventHubConfig[configEventHubNameSelector]
            const connectionString = eventHubConfig[configConnectionStringSelector];
            client = EventHubClient.createFromConnectionString(connectionString, eventHubName);
        } else {
            // No EventHub configuration found in config
            throw 'No configuration for EventHub was found!'
        }
        if ('name' in packageInfo && 'version' in packageInfo) {
            clientName = packageInfo.name;
            var tmp = packageInfo.version.split('.');
            version[eventListData.VERSION_RELEASE] = tmp[0];
            version[eventListData.VERSION_PATCH] = tmp[1];
            version[eventListData.VERSION_HOTFIX] = tmp[2];
        } else {
            throw 'No package info was found!';
        }
        try {
            if (fileSystem.readFile(lastEventFile, 'utf8', (err, data) => {
                const file = parseInt(data);
                if (!!file) {
                    lastEvent = file;
                }
            }));
        } catch (e) { console.log(e); }
    },
    'error': (messages, cause) => {
        sendStatusResponse(eventListData.STATUS_CODES.ERROR, messages, cause);
    },
    'send': (name, data, previousEvent) => {
        requireConfigure()
        var body = {}
        body[eventNameKey] = name;
        body[eventDataKey] = (!!data ? data : {});
        body[eventDataKey][eventListData.SENDER] = clientName;
        if (!!previousEvent && !!previousEvent.length) {  
            body[eventDataKey][eventListData.PREVIOUS_EVENT] = previousEvent;
        }
        const message = {};
        message[bodyKey] = body;
        return client.send(message, partitionId);
    },
    'start': (onEvent, onError, onStatusRequest, onVersionRequest) => {
        requireConfigure();
        if (!onError) onError = (error) => {
            console.log(error);
            module.exports.error(eventListData.STATUS_CODES.ERROR, error);
        }
        const onMessage = function(message) {
            try {
                const time = parseInt(message['annotations']['x-opt-enqueued-time']);
                // Check if event had happened _or not_
                if (time > lastEvent) {
                    lastEvent = time;
                    saveLastEvent(time);
                    const body = message[bodyKey];
                    if (body != null && eventNameKey in body) {
                        var name = body[eventNameKey];
                        var data = body[eventDataKey];
                        if (name === eventList.STATUS_REQUEST) {
                            if (!!onStatusRequest) {
                                onStatusRequest(body);
                            } else {
                                sendStatusResponse(eventListData.STATUS_CODES.OK, [], body);
                            }
                        }
                        if (name === eventList.VERSION_REQUEST) {
                            if (!!onVersionRequest) {
                                onVersionRequest(body) 
                            } else {
                                sendVersionResponse(body);
                            }
                        }
                        onEvent(name, data, time);
                    }
                }   
            } catch (e) { 
                onError(e);
             }
        }
        receiver = client.receive(partitionId, onMessage, onError);
        sendVersionResponse();
    },
    
    'stop': () => {
        requireConfigure();
        if (receiver) {
            sendStatusResponse(eventListData.STATUS_CODES.OFFLINE, 'eventHub.stop was called.')
            receiver.stop();
        }
    }
}

function requireConfigure() {
    if (!client) throw Error('Call configure before start');
}

function saveLastEvent(timeUnix) {
    fileSystem.writeFile(lastEventFile, timeUnix, 'utf8', (err) => {
        if (err) throw err;
    });
}

/**
 * 
 * @param {String} statusCode As per EventHubDataList.STATUS_CODES
 * @param {String|Array} messages String or array of strings with additional info regarding the status. Note: these will not be taken into account when the StatusCode is OK.
 * @param {Object} previousEvent 
 */
function sendStatusResponse(statusCode, messages, previousEvent) {
    data = {};
    data[eventListData.STATUS_CODE] = statusCode;
    if (statusCode !== eventListData.STATUS_CODES.OK && !!messages) 
        data[eventListData.ERROR_MESSAGE] = messages;
    module.exports.send(eventList.STATUS_RESPONSE, data, previousEvent);
}

function sendVersionResponse(previousEvent) {
    data = JSON.parse(JSON.stringify(version));
    module.exports.send(eventList.VERSION_RESPONSE, data, previousEvent).catch((reason) => {
        console.log(reason);
    });
}