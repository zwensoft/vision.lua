--[[

Copyright 2016 The Node.lua Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS-IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

--]]
local timer  = require('timer')
local utils  = require('utils')
local json   = require('json')


local exports = {}

local TAG = 'sscp'

-------------------------------------------------------------------------------
-- MQTT publish

local function mqtt_publish(clientId, dataTopic, content, callback)
    local mqttUrl = 'mqtt://iot.sae-sz.com:1883'

    local onMessage = function(topic, payload)
		console.log(TAG, 'message', topic, payload)
	end

    local timeoutTimer = nil

    local mqtt  = require('mqtt')

    local options = { callback = onMessage, clientId = clientId }
	client = mqtt.connect(mqttUrl, options)
	client.debugEnabled = true
    
   	client:on('connect', function(connack)
        client:publish(dataTopic, content, { qos = 1 }, function(publish, err)
            if (timeoutTimer) then
                clearTimeout(timeoutTimer)
                timeoutTimer = nil
            end

            if (callback) then
                callback(publish, err)
            end

            client:close()
	  	end)
	end)

	client:on('error', function(errInfo)
		console.log(TAG, 'event', 'error', errInfo)
	end)

    -- set timeout
    timeoutTimer = setTimeout(10 * 1000, function()
        print(TAG, 'send timeout!')
        client:close()
    end)
end

function exports.publish(options, reported)
    options = options or {}
    --console.log(TAG, 'publish', options, reported)

    local dataTopic = '/device/data'

    local seq = 1
    local timestamp = os.time()
    local data = {
        reported 	= reported,
        seq 		= seq,
        timestamp 	= timestamp
    }

    --console.log(TAG, 'data', data)

    local deviceKey = options.deviceKey or ''
    local deviceId  = options.deviceId  or ''

    local key = tostring(timestamp)
    local hash = utils.bin2hex(utils.md5(deviceKey .. key))

	local message = {
	    device_id   = deviceId,
	    device_key  = deviceKey,
        key         = key,
        hash        = hash,
        state       = {},
        version     = 10
    }

    table.insert(message.state, data)
    --console.log(TAG, 'message', message)

    local content = json.stringify(message)
    mqtt_publish(deviceId, dataTopic, content, function(publish, err)
        if (err) then
            print('MQTT message sent failed:')
            console.log(TAG, err)

        else
            print('MQTT message sent complete:')
            console.log(TAG, data)
        end
    end)
end

return exports

