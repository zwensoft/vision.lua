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
local path 	 = require('path')
local thread = require('thread')
local core 	 = require('core')
local utils  = require('utils')
local timer  = require('timer')
local json   = require('json')
local mqtt   = require('mqtt')
local conf   = require('ext/conf')

local exports = {}


local vision = {
	deviceId 		= '12345678',
	deviceName 		= "IPCAM",
	deviceType 		= "camera",
	deviceVersion 	= '1.1.1.1',
	mqttVersion		= 1,
	mqttName 		= "node.lua-v1",
	url 			= 'mqtt://127.0.0.1:1883/',
	expires 		= 30
}

local function sscp_load_settings()
	local profile, err = conf.load("vision.conf")
	if (profile) then
		vision.conffile 	= profile.filename
		vision.deviceId 	= profile:get("vision.deviceId")  	or '12345678'
		vision.deviceName 	= profile:get("vision.deviceName") 	or "IPCAM"
		vision.deviceType   = profile:get("vision.deviceType")  or "camera"
		vision.expires 		= profile:get("vision.expires")  	or 30
		vision.url  		= profile:get("vision.url")  		or 'mqtt://127.0.0.1:1883/'

		console.log(vision)
	end
end

sscp_load_settings()

local SscpServer = core.Emitter:extend()
exports.SscpServer = SscpServer

function SscpServer:initialize(socket)
	self.debugEnabled = false

	if (not self.status) then
		status = {}
		status["/status/"] 			= self.onStatusDevice
		status["/status/device"] 	= self.onStatusDevice
		self.status = status;
	end

	if (not self.control) then
		control = {}
		control["/control/device"] 	= self.onControlSnapshot
		control["/control/update"] 	= self.onControlUpdate
		self.control = control;
	end

	if (not self.notify) then
		notify = {}
		notify["/notify/"] 			= self.onNotifySnapshot
		notify["/notify/device"] 	= self.onNotifySnapshot
		self.notify = notify;
	end
end

function SscpServer:onControlSnapshot(topic, url, seq, params)
	return { ret = 0 }
end

function SscpServer:onControlUpdate(topic, url, seq, params)
	os.execute("lpm update &")
end

function SscpServer:onDebugDump(...)
	if (self.debugEnabled) then
		console.log('onDebugDump', ...)
	end
end

function SscpServer:onNotifySnapshot(topic, url, seq, params)
	return { ret = 0 }
end

function SscpServer:onJsonMessage(topic, data)
	self:onDebugDump('onJsonMessage', topic, data)

	local url    = data["path"] or ""
	local params = data["data"] or {}
	local seq    = data["seq"] or 0	
	local source = data["source"] or ""
	self:onRequest(source, url, seq, params);
end

function SscpServer:onRequestStatus(topic, url, seq, params)
	self:onDebugDump('onRequestStatus', url, params)

	local status = self.status[url]
	if (status) then
		status(self, topic, url, seq, params)
	end
end

function SscpServer:onRequestControl(topic, url, seq, params)
	self:onDebugDump('onRequestControl', url, params)

	local control = self.control[url]
	if (not control) then
		return
	end

	local data = control(self, topic, url, seq, params)
	if (data) then
		self:sendRequest(topic, url, data, seq)
	end
end

function SscpServer:onRequestNotify(topic, url, seq, params)
	self:onDebugDump('onRequestNotify', url, params)

	local notify = self.notify[url]
	if (notify) then
		notify(self, topic, url, seq, params)
	end
end

function SscpServer:onRequest(source, url, seq, params)
	if (type(source) ~= 'string') or (#source <= 0) then
		self:onDebugDump('onRequest: invalid source')
		return

	elseif (type(url) ~= 'string') then
		self:onDebugDump('onRequest: invalid url')
		return		
	end
	--console.log('sscp_on_request', url, params)

	local topic = "/peers/" .. source

	if (url:startsWith("/status/")) then
		self:onRequestStatus(topic, url, seq, params)

	elseif (url:startsWith("/control/")) then
		self:onRequestControl(topic, url, seq, params)

	elseif (url:startsWith("/notify/")) then
		self:onRequestNotify(topic, url, seq, params)				
	end
end

function SscpServer:onRawMessage(topic, payload)
	if (not payload) then
		self:onDebugDump('onRawMessage: invalid payload')
		return
	end

	if (payload:byte(1) == 123) then -- char '{'
		local data = json.parse(payload)
		if (not data) then
			self:onDebugDump('onRawMessage: invalid json format')
			return
		end	

		if (data) then
			self:onJsonMessage(topic, data)

		else
			self:onDebugDump('invalid json message', topic, payload)
		end

	else
		self:onDebugDump('raw message', topic, payload)
	end
end

function SscpServer:onStatusDevice(topic, url, seq, params)
	local data = { name = "IPCAM" }
	if (not data) then
		return
	end

	self:onDebugDump('onStatusDevice:', data)

	self:sendRequest(topic, url, data, seq)
end

function SscpServer:sendRegister()
	local data = {}
	data.name 		= vision.deviceName
	data.type 		= vision.deviceType
	data.uid 		= vision.deviceId
	data.version 	= vision.deviceVersion
	data.expires	= vision.expires

	self:sendRequest("/register", "/register", data)
end

function SscpServer:sendRegisterHeartBeat()
	local data = {}
	data.uid = vision.deviceId

	self:sendRequest("/register", "/register/ping", data)
end

function SscpServer:sendRequest(topic, path, data, seq)
	if (type(topic) ~= 'string') or (#topic <= 0) then
		self:onDebugDump('sendRequest', "invalid topic name")
		return
	end

	console.log('topic', topic);

	if (not data) then
		self:onDebugDump('sendRequest', "invalid message data")
		return
	end

	local request = {}
	request.client  = vision.mqttName 
	request.data 	= data or {}
	request.path 	= path
	request.v    	= vision.mqttVersion

	if (seq) then
		request.seq = seq 

	else
		request.seq = vision.seq or 1 
		vision.seq 	= request.seq + 1	
	end

	self:onDebugDump('sendRequest', json.stringify(request))

	local client = vision.mqtt_client
	if (not client) or (not client.connected) then
		self:onDebugDump('sendRequest', "not connected!")
		return
	end

	client:publish(topic, json.stringify(request))
end

local function mqtt_client_init()
	local url = vision.url
	local options = { 
		clientId = vision.deviceId,
		keepalive = 20
	}
	local client = mqtt.connect(url, options)
	client.callback = function(topic, payload) 
		local sscp = vision.sscp_client
		if (sscp) then
			sscp:onRawMessage(topic, payload);
		end
	end
	client.debugEnabled = true
	
	--console.log(client)

	client:on('connect', function()
		print('connect')

		local client = vision.mqtt_client
		client:subscribe({'/peers/' .. vision.deviceId})
	end)

	client:on('offline', function()
		print('offline')
	end)

	client:on('close', function()
		print('close')
	end)	

	client:on('error', function(errInfo)
		print('error', errInfo)
	end)	

	client:on('reconnect', function()
		print('reconnect')
	end)

	print("start connect")
	local ret = client:connect(vision.deviceId)

	vision.mqtt_client = client
	vision.sscp_client = SscpServer:new()
end

local function mqtt_client_connect()
	local client = vision.mqtt_client
	if (client.connected) then
		return
	end

	--console.log(client)
	client.callback = function(topic, payload) 
		local sscp = vision.sscp_client
		if (sscp) then
			sscp:onRawMessage(topic, payload);
		end
	end
	client:connect(vision.deviceId)
end

local function mqtt_on_timer()
	if (vision.mqtt_client == nil) then
		mqtt_client_init()

	else
		local now = process.uptime()
		local span = now - (vision.registerTime or 0)
		if (span < vision.expires) then
			return
		end

		local sscp = vision.sscp_client

		vision.registerTime = now

		if (not sscp) then
			return

		elseif (vision.register) then
			sscp:sendRegisterHeartBeat()
		else 
			sscp:sendRegister()
		end
	end
end

exports.options = vision
exports.onTimer = mqtt_on_timer

return exports
